# import pyarrow.parquet as pq
import pyarrow as pa
import pyarrow.flight
import pandas as pd
import numpy as np

df = pd.DataFrame({'col1': [-1, np.nan, 2.5], 'col2': ['foo', 'bar', 'baz'], 'col3': [True, False, True]})
data_table = pa.Table.from_pandas(df)


def main():
    # location = pa.flight.Location.for_grpc_tcp("localhost", 50051)
    client = pa.flight.connect("grpc://[::1]:50051")
    upload_descriptor = pa.flight.FlightDescriptor.for_path("kutas")
    writer, _ = client.do_put(upload_descriptor, data_table.schema)
    writer.write_table(data_table)
    writer.write_table(data_table)


if __name__ == '__main__':
    main()
