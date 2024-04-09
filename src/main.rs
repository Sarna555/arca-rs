use std::pin::Pin;

use arrow_array::RecordBatch;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::error::FlightError;
use arrow_flight::flight_descriptor::DescriptorType;
use arrow_flight::utils::flight_data_to_arrow_batch;
use arrow_schema::{Schema, SchemaRef};
use datafusion::arrow::util::pretty;
use std::cell::RefCell;
use std::sync::Arc;
use std::sync::RwLock;

use futures::Stream;
use futures::StreamExt;
use futures::TryStreamExt;
use std::collections::HashMap;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};

use arrow_flight::{
    flight_service_server::FlightService, flight_service_server::FlightServiceServer, Action,
    ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo, HandshakeRequest,
    HandshakeResponse, PutResult, SchemaResult, Ticket,
};

#[derive(Clone)]
pub struct STable {
    records: Vec<RecordBatch>,
    name: String,
    // schema: Schema,
}

impl STable {
    fn new(name: String) -> STable {
        STable {
            records: vec![],
            name: String::from(name),
        }
    }
}

#[derive(Clone)]
pub struct TableStorage {
    tables: HashMap<String, Vec<RecordBatch>>,
    // fn append(batch: RecordBatch)
    // {
    //     tables.insert(batch.
    // }
}

impl TableStorage {
    fn new() -> TableStorage {
        TableStorage {
            tables: HashMap::new(),
        }
    }
}

// #[derive(Clone)]
pub struct FlightServiceImpl {
    table_storage: RwLock<TableStorage>,
}

impl FlightServiceImpl {
    fn new() -> FlightServiceImpl {
        FlightServiceImpl {
            table_storage: RwLock::new(TableStorage::new()),
        }
    }
}

#[tonic::async_trait]
impl FlightService for FlightServiceImpl {
    type HandshakeStream =
        Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send + Sync + 'static>>;
    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("Implement handshake"))
    }
    type ListFlightsStream =
        Pin<Box<dyn Stream<Item = Result<FlightInfo, Status>> + Send + Sync + 'static>>;
    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("Implement list_flights"))
    }
    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("Implement get_flight_info"))
    }
    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("Implement get_schema"))
    }
    type DoGetStream =
        Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send + Sync + 'static>>;

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        println!("do_get");
        let cmd = request.into_inner().to_string();
        println!("cmd: {cmd:?}");
        Err(Status::unimplemented("Implement do_get"))
    }

    type DoPutStream =
        Pin<Box<dyn Stream<Item = Result<PutResult, Status>> + Send + Sync + 'static>>;

    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        println!("do_put");
        // let inner = request.into_inner();
        // if let Some(msg) = inner.message().await? {
        //     let desc = msg.flight_descriptor.as_ref();
        //     println!("kutas: {desc:?}");
        //     println!("msg: {msg:?}");
        // }
        // msg.data_header
        //     .into_iter()
        //     .for_each(|x| println!("kurita: {x}"));
        // let sth: RecordBatch = msg.try_into().unwrap();
        // println!("try: {kk:?}");
        //
        // }
        // let mut req: Vec<_> = inner.try_collect().await?;
        // println!("req: {req:?}");
        // println!("ss: {ss:?}");
        // let mut b = FlightRecordBatchStream::new_from_flight_data(inner.map_err(|e| e.into()));
        // if let Some(schema) = b.schema(){
        //     println!("schema: {schema:?}");
        // }
        // else {
        //     println!("no schema");
        // }
        // // println!("b: {b:?}");
        // while let Some(batch) = b.next().await {
        //     match batch {
        //         Ok(batch) => {
        //             {println!("batch: {batch:?}"); batch. }
        //         }
        //         Err(e) => println!("error: {e}"),
        //     }
        // }
        let mut stream = request.into_inner();
        let flight_data = stream.message().await?.unwrap();
        let mut table_name = String::new();
        {
            let desc = &flight_data.flight_descriptor;
            // println!("desc: {desc:?}");

            table_name = match desc.as_ref().unwrap().r#type() {
                DescriptorType::Path => desc.as_ref().unwrap().path[0].clone(),
                _ => {
                    panic!("not implemented")
                }
            };

            println!("table name: {table_name}");
        }
        // let mut b = FlightRecordBatchStream::new_from_flight_data(stream.map_err(|e| e.into()));
        // while let Some(batch) = b.next().await {
        //     match batch {
        //         Ok(batch) => {
        //             {println!("batch: {batch:?}");}
        //         }
        //         Err(e) => println!("error: {e}"),
        //     }
        // }
        let schema = Arc::new(
            Schema::try_from(&flight_data)
                .map_err(|e| FlightError::DecodeError(format!("Error decoding schema: {e}")))?,
        );
        println!("Schema: {schema:?}");
        // let mut results = vec![];
        let dictionaries_by_field = HashMap::new();
        while let Some(flight_data) = stream.message().await? {
            let record_batch = flight_data_to_arrow_batch(
                &flight_data,
                Arc::clone(&schema),
                &dictionaries_by_field,
            );
            println!("saving");
            if self
                .table_storage
                .read()
                .unwrap()
                .tables
                .contains_key(&table_name)
            {
                self.table_storage
                    .write()
                    .unwrap()
                    .tables
                    .get_mut(&table_name)
                    .unwrap()
                    .push(record_batch.map_err(|e| Status::unknown(e.to_string()))?);
            }
            // results.push(record_batch);
        }
        //
        // println!("results: {results:?}");

        // print the results
        // pretty::print_batches(&results)?;
        Err(Status::unimplemented("Implement do_put"))
    }

    type DoExchangeStream =
        Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send + Sync + 'static>>;

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("Implement do_exchange"))
    }

    type DoActionStream =
        Pin<Box<dyn Stream<Item = Result<arrow_flight::Result, Status>> + Send + Sync + 'static>>;

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("Implement do_action"))
    }

    type ListActionsStream =
        Pin<Box<dyn Stream<Item = Result<ActionType, Status>> + Send + Sync + 'static>>;

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("Implement list_actions"))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:50051".parse().unwrap();
    let service = FlightServiceImpl::new();

    let svc = FlightServiceServer::new(service);

    println!("listening on {addr}");
    Server::builder().add_service(svc).serve(addr).await?;

    Ok(())
}
