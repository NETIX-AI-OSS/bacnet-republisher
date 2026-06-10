fn main() {
    println!("cargo:rerun-if-changed=assets/app-icon.ico");
    println!("cargo:rerun-if-changed=assets/app-icon.png");

    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() != Ok("windows") {
        return;
    }

    let mut resource = winresource::WindowsResource::new();
    resource.set("FileDescription", "NETIX BACnet Republisher");
    resource.set("ProductName", "BACnet Republisher");
    resource.set("CompanyName", "NETIX");
    resource.set_icon("assets/app-icon.ico");
    resource
        .compile()
        .expect("failed to compile Windows executable resources");
}
