use skeptic::*;

fn main() {
    // Add all markdown files in directory "src/".
    let mdbook_files = markdown_files_of_directory("src/");
    println!("{:?}", &mdbook_files);
    generate_doc_tests(&mdbook_files);
}
