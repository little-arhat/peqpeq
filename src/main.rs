use std::env;
use std::collections::VecDeque;

use parquet2::{
    FallibleStreamingIterator,
    page::CompressedPage,
    read::{get_page_iterator, read_metadata},
    write::{
        DynIter,
        DynStreamingIterator, FileWriter, Version,
        WriteOptions,
    },
    error::{Error, Result},
};


struct PageBuffer {
    columns: VecDeque<CompressedPage>,
    current: Option<CompressedPage>,
}

impl PageBuffer {
    pub fn new(columns: VecDeque<CompressedPage>) -> Self {
        Self {
            columns,
            current: None,
        }
    }
}

impl FallibleStreamingIterator for PageBuffer {
    type Item = CompressedPage;
    type Error = Error;

    fn advance(&mut self) -> Result<()> {
        self.current = self.columns.pop_front();
        Ok(())
    }

    fn get(&self) -> Option<&Self::Item> {
        self.current.as_ref()
    }
}


fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();

    let mut in_file = std::fs::File::open(&args[1])?;
    let metadata = read_metadata(&mut in_file)?;
    let num_cols = metadata.schema().fields().len();

    let out_file = std::io::BufWriter::new(std::fs::File::create(&args[2])?);
    let options = WriteOptions {
        write_statistics: false,
        version: Version::V2,
    };
    let schema_to_write = metadata.schema().clone();
    let mut writer = FileWriter::new(out_file, schema_to_write, options, None);

    writer.write(DynIter::new(
        metadata.row_groups
            .iter()
            .map(|group| {
                println!("reading group: {:?}", group);
                let pgs =
                    (0..num_cols)
                        .into_iter()
                        .map(|i| &group.columns()[i])
                        .map(|column_meta| {
                            println!("reading {:?}", column_meta);
                            get_page_iterator(column_meta,
                                               &mut in_file,
                                               None,
                                               vec![],
                                               1024 * 1024)
                                .into_iter()
                                .flatten()
                                .collect::<VecDeque<_>>()
                                .into_iter()
                        })
                        .flatten()
                        .flatten()
                        .collect::<VecDeque<_>>();
                 println!("Read {} pages", pgs.len());
                 let pb = PageBuffer::new(pgs);
                 Ok(DynStreamingIterator::new(pb))
            })))?;

    writer.end(metadata.key_value_metadata)?;

    Ok(())
}
