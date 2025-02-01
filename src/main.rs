extern crate reqwest;

use std::env::join_paths;
use std::fmt::{format, Debug};
use std::fs::File;

use std::io::{Error, Read, Seek, SeekFrom, Write};
use std::sync::{Arc, Mutex};
use std::{io, thread};
use std::cmp::max_by;
use std::thread::JoinHandle;
use reqwest::blocking::{Client, Response};

fn main(){
    let chunk_size:u64 = 1024 * 8;
    if let Ok(data) = concurrent_download(chunk_size,"https://www.stats.govt.nz/assets/Uploads/Annual-balance-sheets/Annual-balance-sheets-2023-provisional/Download-data/accumulation-accounts-2008-2023-provisional.csv"
                                          ,"test.csv"){
        for handler in data  {
            let _ = handler.join().unwrap();
        }
        println!("\ndone");
    }
}

fn concurrent_download(chunk_size:u64,url:&'static str,filename:&str)-> Result<Vec<JoinHandle<Result<(),String>>>,Box<dyn std::error::Error>>{
    let client = Client::new();
    let response:Response = client.head(url).send()?;

    let file_size = response
        .headers()
        .get("content-length")
        .ok_or("content-length missing")?
        .to_str()?
        .parse::<u64>()?;

    println!("file size is {}",file_size);
    let file = File::create(filename)?;
    file.set_len(file_size)?;
    let mut size:f64 = 0f64;
    let file = Arc::new(Mutex::new(file));
    let mut handlers    = vec![];
    for start in (0..file_size).step_by(chunk_size as usize){
        let end = (start + chunk_size -1).min(file_size -1);

        let client = client.clone();
        // println!("{} - {} - {}",size,file_size,start);
        print!("\rDownloading... {:.2} %",(start as f64/file_size as f64)*100f64);

        io::stdout().flush().unwrap();

        let file = Arc::clone(&file);

        let handler = thread::spawn(move ||{
           let response = client.get(url)
               .header("Range",format!("bytes={start}-{end}"))
               .send()
               .map_err(|e| format!("Request failed {}",e))?
               .bytes()
               .map_err(|e| format!("Failed to read Bytes , {}",e))?;


            let mut file = file.lock().map_err(|e| format!("Mutex lock Failed {}",e))?;
            file.seek(SeekFrom::Start(start))
                .map_err(|e| format!("error {e}"))?;
            file.write_all(&response).map_err(|e|format!("error {e}"))?;

            Ok::<(),String>(())
        });
        handlers.push(handler);
    }
    Ok(handlers)
}

