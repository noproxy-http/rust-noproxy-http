/*
 * Copyright 2021, alex at staticlibs.net
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

extern crate lazy_static;
extern crate serde_derive;
extern crate serde_json;

use std::collections::HashMap;
use std::io::Error;
use std::io::ErrorKind;
use std::os::raw::*;
use std::slice;
use std::str;
use std::sync::mpsc;
use std::sync::mpsc::Receiver;
use std::sync::mpsc::Sender;
use std::sync::Mutex;
use std::thread;

use lazy_static::lazy_static;
use serde_derive::Deserialize;
use serde_derive::Serialize;

lazy_static! {
    static ref SENDER: Mutex<Option<Sender<Request>>> = Mutex::new(None);
    static ref RECEIVER: Mutex<Option<Receiver<Request>>> = Mutex::new(None);
    static ref SEND_RESPONSE: Mutex<Option<SendResponseFun>> = Mutex::new(None);
    static ref HANDLER: Mutex<Option<fn(Request)>> = Mutex::new(None);
}

type SendResponseFun = fn(
    request: *mut c_void,
    http_status: c_int,
    headers: *const c_char,
    headers_len: c_int,
    data: *mut c_char,
    data_len: c_int,
) -> c_int;

#[allow(non_snake_case)]
#[derive(Serialize, Deserialize)]
pub struct Metadata {
    uri: String,
    args: String,
    unparsedUri: String,
    method: String,
    protocol: String,
    dataTempFile: Option<String>,
    headers: HashMap<String, String>,
}

pub struct Request {
    handle: u64,
    meta: Metadata,
    data_ptr: u64,
    data_len: usize,
}

pub fn send_response(handle: u64, response_code: u16, headers: &HashMap<String, String>,
                     data_ptr: *mut c_char, data_len: usize) -> Result<(), Error> {
    let send_guard = match SEND_RESPONSE.lock() {
        Ok(guard) => guard,
        Err(err) => {
            let msg = format!("Error accessing send callback, error: [{}]", err);
            return Err(Error::new(ErrorKind::Other, msg));
        }
    };
    let send = match &*send_guard {
        Some(val) => val,
        None => {
            let msg = format!("Error accessing send callback");
            return Err(Error::new(ErrorKind::Other, msg));
        }
    };
    // send
    let headers_json = serde_json::to_vec(&headers)?;
    let headers_ptr = headers_json.as_ptr() as *const c_char;
    let handle_ptr = handle as *mut c_void;
    match send(handle_ptr, response_code as c_int, headers_ptr, headers_json.len() as c_int, data_ptr, data_len as c_int) {
        0 => Ok(()),
        err => {
            let msg = format!("Error sending repsonse, code: [{}]", err);
            Err(Error::new(ErrorKind::Other, msg))
        }
    }
}

// todo
pub fn send_json() -> () {}

pub fn set_handler(handler: fn(Request)) -> Result<(), Error> {
    let mut handler_guard = match HANDLER.lock() {
        Ok(val) => val,
        Err(err) => {
            let msg = format!("Error setting handler, message: [{}]", err);
            return Err(Error::new(ErrorKind::Other, msg))
        }
    };
    let global_hander = &mut *handler_guard;
    *global_hander = Some(handler);
    Ok(())
}

fn default_handler(req: Request) -> () {
    let msg = format!("Hello from Rust, your request was received on path: [{}]\n", req.meta.uri);
    let bytes = msg.as_bytes();
    let data_ptr = unsafe {
        let buf: *mut c_char = libc::malloc(bytes.len()) as *mut c_char;
        let src = bytes.as_ptr() as *mut c_void;
        let dest = buf as *mut c_void;
        libc::memcpy(dest, src, bytes.len());
        buf
    };
    let mut headers: HashMap<String, String> = HashMap::new();
    headers.insert("X-Custom-Rust-Header".to_string(), "foo".to_string());
    let _ = send_response(req.handle, 200, &headers, data_ptr, bytes.len());
}

fn receive_request() -> Result<(), Error> {
    // get receiver
    let recv_guard = match RECEIVER.lock() {
        Ok(guard) => guard,
        Err(err) => {
            let msg = format!("Error accessing receiver, message: [{}]", err);
            return Err(Error::new(ErrorKind::Other, msg));
        }
    };
    let receiver = match &*recv_guard {
        Some(val) => val,
        None => {
            let msg = format!("Error accessing receiver");
            return Err(Error::new(ErrorKind::Other, msg));
        }
    };

    // get handler
    let mut handler_guard = match HANDLER.lock() {
        Ok(val) => val,
        Err(err) => {
            let msg = format!("Error accessing handler, message: [{}]", err);
            return Err(Error::new(ErrorKind::Other, msg));
        }
    };
    let handler = match &*handler_guard {
        Some(val) => val,
        None => {
            let msg = format!("Error accessing handler");
            return Err(Error::new(ErrorKind::Other, msg));
        }
    };

    // receive message
    match receiver.recv() {
        Ok(req) => {
            handler(req);
            Ok(())
        }
        Err(err) => {
            let msg = format!("Queue receive failed, message: [{}]", err.to_string());
            Err(Error::new(ErrorKind::Other, msg))
        }
    }
}

#[no_mangle]
fn bch_initialize(
    response_callback: SendResponseFun,
    handler_config: *const c_char,
    handler_config_len: c_int,
) -> c_int {
    let slice: &[u8] = unsafe {
        slice::from_raw_parts(handler_config as *const u8, handler_config_len as usize)
    };
    let config = match str::from_utf8(slice) {
        Ok(val) => val,
        Err(_) => return 1
    };

    eprintln!("Rust handler init called, config: [{}]", config);

    let (sender, receiver) = mpsc::channel::<Request>();

    // set global sender
    let mut sender_guard = match SENDER.lock() {
        Ok(val) => val,
        Err(_) => return -1
    };
    let global_sender = &mut *sender_guard;
    *global_sender = Some(sender);

    // set global receiver
    let mut receiver_guard = match RECEIVER.lock() {
        Ok(val) => val,
        Err(_) => return -2
    };
    let global_receiver = &mut *receiver_guard;
    *global_receiver = Some(receiver);

    // set global response fun
    let mut send_response_guard = match SEND_RESPONSE.lock() {
        Ok(val) => val,
        Err(_) => return -3
    };
    let global_send_response = &mut *send_response_guard;
    *global_send_response = Some(response_callback);

    // set default handler
    let mut handler_guard = match HANDLER.lock() {
        Ok(val) => val,
        Err(_) => return -4
    };
    let global_hander = &mut *handler_guard;
    *global_hander = Some(default_handler);

    // spawn worker thread
    thread::spawn(|| {
        loop {
            match receive_request() {
                Ok(_) => (),
                Err(err) => eprintln!("{}", err)
            }
        }
    });

    0
}

#[no_mangle]
fn bch_receive_request(
    handle: *mut c_void,
    metadata: *const c_char,
    metadata_len: c_int,
    data: *const c_char,
    data_len: c_int,
) -> c_int {
    let meta_slice: &[u8] = unsafe {
        slice::from_raw_parts(metadata as *const u8, metadata_len as usize)
    };
    let meta = match serde_json::from_slice(meta_slice) {
        Ok(val) => val,
        Err(_) => return 1
    };

    let req = Request {
        handle: handle as u64,
        meta,
        data_ptr: data as u64,
        data_len: data_len as usize,
    };

    eprintln!("Rust request received");

    let guard = match SENDER.lock() {
        Ok(val) => val,
        Err(_) => return -1
    };

    let opt = &*guard;

    if let Some(sender) = opt {
        match sender.send(req) {
            Ok(_) => 0,
            Err(_) => 1
        }
    } else {
        -2
    }
}

#[no_mangle]
fn bch_free_response_data(
    data: *mut c_void
) -> () {
    unsafe {
        libc::free(data);
    }
}
