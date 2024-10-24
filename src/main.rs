use std::fs::File;

use color_eyre::Result;
use parquet::file::{
    metadata::{ParquetMetaData, ParquetMetaDataReader},
    page_encoding_stats::PageEncodingStats,
};
use ratatui::{
    crossterm::event::{self, Event, KeyCode, KeyEventKind},
    layout::{Constraint, Direction, Layout},
    style::{Color, Style, Stylize},
    text::Line,
    widgets::{Bar, BarChart, BarGroup, Block},
    DefaultTerminal, Frame,
};

fn main() -> Result<()> {
    color_eyre::install()?;
    let mut terminal = ratatui::init();

    let path = std::env::args()
        .nth(1)
        .expect("must provide PATH to parquet file");
    let row_group_num = std::env::args()
        .nth(2)
        .expect("arg")
        .parse::<usize>()
        .expect("couldn't parse row group num");

    let col_num = std::env::args()
        .nth(3)
        .expect("arg")
        .parse::<usize>()
        .expect("couldn't parse col num");

    // read the parquet footer
    let file = File::open(&path).expect(&format!("could not open {path}"));
    let mut reader = ParquetMetaDataReader::new().with_page_indexes(true);
    reader.try_parse(&file).expect("could not parse file");
    let metadata = reader.finish().expect("could not finish file");

    let page_datas = vec![get_page_stats(&metadata, row_group_num, col_num)];

    let chart = get_bar_chart(&page_datas);

    let app = App::new(chart);
    let result = app.run(terminal);
    ratatui::restore();
    result
}

#[derive(Default)]
struct PageData {
    num_pages: u64,
    num_values: u64,
}

fn get_bar_chart(page_datas: &Vec<PageData>) -> BarChart<'static> {
    let mut barchart = BarChart::default()
        .block(Block::new().title("columns"))
        .bar_width(1)
        .group_gap(2)
        .bar_gap(0)
        .direction(Direction::Horizontal);

    let colors = vec![Color::LightRed, Color::LightBlue, Color::White];

    let bars: Vec<Bar> = page_datas
        .iter()
        .enumerate()
        .map(|(i, pd)| {
            Bar::default()
                .value(pd.num_pages)
                .text_value(format!("{} pages", pd.num_pages))
                .style(colors[i])
        })
        .collect();

    let bar_group = BarGroup::default()
        .label(Line::from("this is my bar group").centered())
        .bars(&bars);

    barchart.data(bar_group)
}

fn get_page_stats(metadata: &ParquetMetaData, rg: usize, col_num: usize) -> PageData {
    let rg = metadata.row_group(rg);

    let cc_meta = rg.column(col_num);

    let mut page_data = PageData::default();
    match cc_meta.page_encoding_stats() {
        Some(stats) => {
            page_data.num_pages = stats.len() as u64;
            page_data.num_values = stats.iter().map(|x| x.count as u64).sum();
        }
        None => {}
    }
    page_data
}

struct App {
    should_exit: bool,
    chart: BarChart<'static>,
}

impl App {
    const fn new(chart: BarChart<'static>) -> Self {
        Self {
            should_exit: false,
            chart,
        }
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
            if key.kind == KeyEventKind::Press && key.code == KeyCode::Char('q') {
                self.should_exit = true;
            }
        }
        Ok(())
    }

    fn draw(&self, frame: &mut Frame) {
        use Constraint::{Fill, Length, Min};
        let vertical = Layout::vertical([Length(1), Fill(1), Min(20)]).spacing(1);
        let [title, top, bottom] = vertical.areas(frame.area());

        frame.render_widget("TITLE".bold().into_centered_line(), title);
        frame.render_widget(&self.chart, top);
    }
}
