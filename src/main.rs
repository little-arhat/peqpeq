use std::env;

use parquet2::{
    page::CompressedPage,
    read::{get_page_iterator, read_metadata},
    error::Result,
};


fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();
    let path = &args[1];

    let mut file = std::fs::File::open(path)?;
    let metadata = read_metadata(&mut file)?;
    let num_cols = metadata.schema().fields().len();

    for (i, group) in metadata.row_groups.iter().enumerate() {
        println!(
            "Group: {:<10}Rows: {:<15}Bytes: {:}",
            i,
            group.num_rows(),
            group.total_byte_size()
        );
        println!("\\m/");

        for column in 0..num_cols {
            let column_meta = &group.columns()[column];
            println!("{:?}", column_meta);
            let iter = get_page_iterator(column_meta,
                                         &mut file,
                                         None,
                                         vec![],
                                         1024 * 1024)?;
            for (page_ind, compressed_page) in iter.enumerate() {
                let compressed_page = compressed_page?;
                print!(
                    "\nPage: {:<10}Column: {:<15}; ",
                    page_ind,
                    column
                );
                match compressed_page {
                    CompressedPage::Dict(dict_page) => {
                        println!("dict page; sorted: {}", dict_page.is_sorted);
                        // the first page may be a dictionary page, which needs to be deserialized
                        // depending on your target in-memory format, you may want to deserialize
                        // the values differently...
                        // let page = deserialize_dict(&page)?;
                        // dict = Some(page);
                    }
                    CompressedPage::Data(_data_page) => {
                        println!("data page");
                        //let _array = deserialize(&page, dict.as_ref())?;
                    }
                }

            }
        }
    }

    Ok(())
}
