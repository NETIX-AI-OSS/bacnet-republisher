fn main() {
    configure_windows_resources();
}

#[cfg(windows)]
fn configure_windows_resources() {
    let mut resource = winresource::WindowsResource::new();
    resource.set("FileDescription", "NETIX BACnet Republisher");
    resource.set("ProductName", "BACnet Republisher");
    resource.set("CompanyName", "NETIX");
    resource
        .compile()
        .expect("failed to compile Windows executable resources");
}

#[cfg(not(windows))]
fn configure_windows_resources() {}
