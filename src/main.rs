mod app;
mod bacnet;
mod config;
mod log;
mod model;
mod mqtt;
mod network;
mod topic;
mod value;
mod worker;

fn main() -> iced::Result {
    app::BacnetRepublisher::run()
}
