#![cfg_attr(all(windows, not(debug_assertions)), windows_subsystem = "windows")]

fn main() -> iced::Result {
    bacnet_republisher::app::BacnetRepublisher::run()
}
