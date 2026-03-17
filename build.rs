fn main() -> Result<(), Box<dyn std::error::Error>> {
    let home = std::env::var("HOME").expect("HOME environment variable not set");
    let proto_path = format!("{}/dev/ai/proto/v1/retrieval.proto", home);
    let proto_include = format!("{}/dev/ai/proto", home);

    tonic_build::configure()
        .build_server(false)
        .compile_protos(&[proto_path], &[proto_include])?;

    Ok(())
}
