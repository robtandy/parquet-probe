use std::fs::File;

use argh::FromArgs;
use color_eyre::Result;
use parquet::column::page::Page;
use parquet::file::{
    metadata::{ParquetMetaData, ParquetMetaDataReader},
    page_encoding_stats::PageEncodingStats,
    reader::FileReader,
    serialized_reader::SerializedFileReader,
    statistics::Statistics,
};
use ratatui::layout::Rect;
use ratatui::style::palette::tailwind::{self, Palette};
use ratatui::text::{Text, ToLine};
use ratatui::widgets::{Paragraph, Wrap};
use ratatui::{
    crossterm::event::{self, Event, KeyCode, KeyEventKind},
    layout::{Constraint, Direction, Layout},
    style::{Color, Style, Stylize},
    text::Line,
    widgets::{Bar, BarChart, BarGroup, Block},
    DefaultTerminal, Frame,
};

#[derive(FromArgs, Clone)]
/// Visualize metadata from one or more parquet files
struct Args {
    #[argh(positional, greedy)]
    paths: Vec<String>,

    #[argh(option)]
    /// row group to examine
    row_group: usize,

    #[argh(option)]
    /// column number to examine
    column: usize,
}

fn main() -> Result<()> {
    color_eyre::install()?;

    let args = argh::from_env();

    let app = App::new(args);

    let mut terminal = ratatui::init();
    let result = app.run(terminal);
    ratatui::restore();
    result
}

struct ParqFile {
    path: String,
    pages: Vec<Page>,
    current_row_group: usize,
    current_col: usize,
    metadata_reader: ParquetMetaDataReader,
    reader: Box<dyn FileReader>,
}

impl ParqFile {
    fn new(path: &str) -> Self {
        // read the parquet footer
        let file = File::open(&path).expect(&format!("could not open {path}"));
        let mut metadata_reader = ParquetMetaDataReader::new().with_page_indexes(true);
        metadata_reader
            .try_parse(&file)
            .expect("could not parse file");
        let metadata = metadata_reader.finish().expect("could not finish file");

        let reader = SerializedFileReader::new(file).expect("could not create reader");

        let pages = Self::get_pages(&reader, 0, 0);

        Self {
            path: path.into(),
            pages,
            current_row_group: 0,
            current_col: 0,
            metadata_reader,
            reader: Box::new(reader),
        }
    }

    fn reload_pages(&mut self) {
        self.pages = Self::get_pages(
            self.reader.as_ref(),
            self.current_row_group,
            self.current_col,
        );
    }

    fn get_pages(reader: &dyn FileReader, row_group_num: usize, col_num: usize) -> Vec<Page> {
        reader
            .get_row_group(row_group_num)
            .expect("couldn't read row group")
            .get_column_page_reader(col_num)
            .expect("couldn't get column page reader")
            .collect::<Result<Vec<Page>, _>>()
            .expect("couldn't read pages")
    }
}

struct App {
    should_exit: bool,
    files: Vec<ParqFile>,
    args: Args,
    palettes: Vec<Palette>,
    max_col_display_length: u32,
    focused_file: usize,
}

impl App {
    fn new(args: Args) -> Self {
        let mut app = Self {
            should_exit: false,
            files: args.paths.iter().map(|p| ParqFile::new(p)).collect(),
            args: args.clone(),
            palettes: vec![
                tailwind::ORANGE,
                tailwind::PINK,
                tailwind::PURPLE,
                tailwind::VIOLET,
                tailwind::SKY,
            ],
            max_col_display_length: 0,
            focused_file: 0,
        };

        app.files.iter_mut().for_each(|pf| {
            pf.current_row_group = args.row_group;
            pf.current_col = args.column;
            pf.reload_pages();
        });
        app.recalculate();
        app
    }

    fn recalculate(&mut self) {
        self.max_col_display_length = self
            .files
            .iter()
            .map(|pf| {
                pf.pages
                    .iter()
                    .map(|page| page.buffer().len())
                    .sum::<usize>()
            })
            .max()
            .expect("cannot calculate display height") as u32;
    }

    fn run(mut self, mut terminal: DefaultTerminal) -> Result<()> {
        while !self.should_exit {
            terminal.draw(|frame| self.draw(frame))?;
            self.handle_events()?;
        }
        Ok(())
    }

    fn handle_events(&mut self) -> Result<()> {
        if let Event::Key(key) = event::read()? {
            let pf = &mut self.files[self.focused_file];

            if key.kind == KeyEventKind::Press && key.code == KeyCode::Char('q') {
                self.should_exit = true;
            } else if key.kind == KeyEventKind::Press && key.code == KeyCode::Esc {
                self.should_exit = true;
            } else if key.kind == KeyEventKind::Press && key.code == KeyCode::Up {
                pf.current_row_group += 1;
                pf.reload_pages();
                self.recalculate();
            } else if key.kind == KeyEventKind::Press && key.code == KeyCode::Down {
                pf.current_row_group -= 1;
                pf.reload_pages();
                self.recalculate();
            } else if key.kind == KeyEventKind::Press && key.code == KeyCode::Left {
                pf.current_col -= 1;
                pf.reload_pages();
                self.recalculate();
            } else if key.kind == KeyEventKind::Press && key.code == KeyCode::Right {
                pf.current_col += 1;
                pf.reload_pages();
            } else if key.kind == KeyEventKind::Press && key.code == KeyCode::Tab {
                self.focused_file = (self.focused_file + 1) % self.files.len();
            }
        }
        Ok(())
    }

    fn draw(&self, frame: &mut Frame) {
        use Constraint::{Fill, Length, Min, Percentage, Ratio};
        let vertical = Layout::vertical([Length(4), Fill(1)]).spacing(1);
        let [header_area, center_area] = vertical.areas(frame.area());

        let labels = ["A", "B", "C", "D", "E"];

        //let header = Paragraph::new("parquet-probe".bold().into_centered_line());

        let header = Block::bordered()
            .bg(Color::Black)
            .fg(Color::White)
            .title("parquet-probe".bold().into_centered_line());
        //let title_area = block.inner(header_area);
        //frame.render_widget(block, header_area);

        let file_header = Paragraph::new(
            self.files
                .iter()
                .enumerate()
                .map(|(i, pf)| {
                    Line::from(format!("File {}: {}", labels[i], pf.path))
                        .bg(self.palettes[i].c900)
                        .fg(self.palettes[i].c100)
                })
                .collect::<Vec<_>>(),
        );

        let block_area = header.inner(header_area);

        frame.render_widget(header, header_area);
        frame.render_widget(file_header, block_area);

        let constraints = self
            .files
            .iter()
            .map(|pf| Ratio(1, self.files.len() as u32))
            .collect::<Vec<_>>();

        let horizontal = Layout::horizontal(constraints).spacing(1);

        horizontal
            .split(center_area)
            .into_iter()
            .enumerate()
            .for_each(|(i, column_area)| {
                let mut title = Line::from(format!(
                    "   File:{} Row Group: {} Column: {} Pages:{}  ",
                    labels[i].bold(),
                    self.files[i].current_row_group,
                    self.files[i].current_col,
                    self.files[i].pages.len(),
                ))
                .centered();

                if (i == self.focused_file) {
                    title = title.bg(self.palettes[i].c900);
                }

                let block = Block::bordered()
                    .style(Style::reset())
                    .border_style(Style::default())
                    .fg(self.palettes[i].c100)
                    .title(title);
                let inner = block.inner(*column_area);
                frame.render_widget(block, *column_area);

                self.draw_column(&self.files[i], &self.palettes[i], inner, frame)
            });
    }

    fn draw_column(
        &self,
        parqfile: &ParqFile,
        palette: &Palette,
        column_area: Rect,
        frame: &mut Frame,
    ) {
        use Constraint::{Fill, Length, Min, Percentage, Ratio};
        // fix me, check that page size doesn't overflow u16
        let constraints = parqfile
            .pages
            .iter()
            .map(|page| Ratio(page.buffer().len() as u32, self.max_col_display_length));

        let colors = [palette.c950, palette.c800];
        let foregrounds = [palette.c100];

        let vertical = Layout::vertical(constraints).spacing(0);
        vertical
            .split(column_area)
            .into_iter()
            .enumerate()
            .for_each(|(i, page_area)| {
                let page = &parqfile.pages[i];
                let [left, right] = Layout::horizontal([Percentage(20), Percentage(80)])
                    .spacing(1)
                    .areas(*page_area);
                //let block_left = Block::new().bg(colors[i % colors.len()]);
                let block_left = Paragraph::new(format!("#{} {}b", i, page.buffer().len()))
                    .bg(colors[i % colors.len()])
                    .fg(foregrounds[i % foregrounds.len()]);
                let right_content = page_text(page);
                frame.render_widget(block_left, left);
                frame.render_widget(right_content, right);
            });
    }
}

fn page_text(page: &Page) -> Paragraph {
    Paragraph::new(match page {
        Page::DataPage { .. } => data_page_text(page),
        Page::DataPageV2 { .. } => format!("DataPageV2\n"),
        Page::DictionaryPage { .. } => dict_page_text(page),
    })
    .wrap(Wrap { trim: true })
}

fn dict_page_text(page: &Page) -> String {
    if let Page::DictionaryPage {
        buf,
        num_values,
        encoding,
        is_sorted,
    } = page
    {
        format!(
            "DictionaryPage[{}], num_values:{}, sorted:{}",
            encoding, num_values, is_sorted
        )
    } else {
        "Unexpected Pages Type".into()
    }
}

fn data_page_text(page: &Page) -> String {
    if let Page::DataPage {
        buf,
        num_values,
        encoding,
        def_level_encoding,
        rep_level_encoding,
        statistics,
    } = page
    {
        format!(
            "DataPage [{}], values:{}, page stats:{}",
            encoding,
            num_values,
            statistics
                .as_ref()
                .map_or("N/A".into(), |s| page_stats_text(s)),
        )
    } else {
        "Unexpected Pages Type".into()
    }
}

fn page_stats_text(stats: &Statistics) -> String {
    format!(
        "nulls: {}",
        stats
            .null_count_opt()
            .map_or("n/a".to_string(), |nc| nc.to_string())
    )
}
