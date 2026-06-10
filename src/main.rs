#![cfg_attr(windows, windows_subsystem = "windows")]

fn main() -> iced::Result {
    bacnet_republisher::app::BacnetRepublisher::run()
}
