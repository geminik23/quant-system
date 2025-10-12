use std::path::Path;

fn main() {
    let proto_file = "proto/market.proto";
    let include_dir = "proto";
    let out_dir = "src/grpc"; // keep in source so include! path works

    println!("cargo:rerun-if-changed={proto_file}");
    println!("cargo:rerun-if-changed={include_dir}");

    if !Path::new(proto_file).exists() {
        panic!("Proto file not found: {proto_file}");
    }
    if !Path::new(include_dir).exists() {
        panic!("Proto include dir not found: {include_dir}");
    }
    // Ensure output directory exists (tonic_build does not auto-create arbitrary source dirs)
    if !Path::new(out_dir).exists() {
        std::fs::create_dir_all(out_dir).expect("Failed to create out_dir src/grpc");
    }

    if let Err(e) = tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .out_dir(out_dir)
        .compile_protos(&[proto_file], &[include_dir])
    {
        panic!("Failed to compile protos: {e:?}");
    }
}
