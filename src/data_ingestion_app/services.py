import datetime
from multiprocessing import Process, Queue, cpu_count
import os, uuid
from data_ingestion_app.constants import FilePaths



OUTPUT_FILE_PATH = f"{FilePaths.OUTPUT_FILE_NAME}_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.sql"


class DataParser:
    def __init__(self, block):
        self.block = block
    
    
    def parse(self):
        """
        Parses the data file and returns a list of dictionaries.
        Each dictionary represents a row in the data file.
        """
        data = []
        nmi = None
        interval = None
        
        for line in self.block:
            # Assuming the data is comma-separated
            row = line.strip()
            
            if not row:
                continue
            if row.startswith('200'):
                try:
                    # Extract the NMI and interval from the header
                    parts = row.split(',')
                    nmi = parts[1]
                    interval = int(parts[8])
                except Exception as e:
                    # Print the error
                    print(f"Error parsing 200 line: {line}. Error: {e}")
                    nmi = None
                    interval = None
                    continue
                
            elif row.startswith('300'):
                if nmi is None or interval is None:
                    # If we don't have a valid NMI or interval, skip this line
                    print(f"Skip parsing 300 line: {line}. Error: Missing nmi or interval.")
                    continue
                
                try:
                    parts = row.split(',')
                    timestamp = parts[1]
                    
                    # Convert the timestamp to a datetime object
                    timestamp = datetime.datetime.strptime(timestamp, '%Y%m%d')
                   
                    # Calculate the number of intervals per day
                    number_of_intervals_per_day = round(24 * 60 / interval)
                    
                    # Calculate the consumption
                    readings = parts[2:2 + number_of_intervals_per_day]
                    consumption = self.calculate_consumption(values=readings)   
                    
                    id  = uuid.uuid4()
                    if self.is_nmi_timestamp_unique(nmi, timestamp, data):
                        data.append({
                            'id': id,
                            'nmi': nmi,
                            'timestamp': timestamp,
                            'consumption': consumption
                        })
                    else:
                        # Duplicate entry found, skip this line
                        print(f"Skip parsing 300 line: {line}. Error: Duplicate entry found for NMI: {nmi} at timestamp: {timestamp}. Skipping.")
                
                except Exception as e:
                    # Print the error
                    print(f"Error parsing 300 line: {line}. Error: {e}")
                    continue
        
        return data
    
    def calculate_consumption(self, values):
        """
        Calculates the total consumption based on interval readings.
        """
        clean_values = [float(value) for value in values]
        return round(sum(clean_values), 2)
    
    def is_nmi_timestamp_unique(self, nmi, timestamp, data):
        """
        Checks if the NMI and timestamp combination is unique in the data.
        """
        for parsed_dict in data:
            if parsed_dict['nmi'] == nmi and parsed_dict['timestamp'] == timestamp:
                return False
        return True
    
    
class DDLScriptCreatorForMeterReadings:
    def __init__(self, parsed_data, output_file_path):
        self.data = parsed_data
        self.output_file_path = output_file_path
    
    def insert_values_script(self):
        """
        Generates a DDL script to insert values into the MeterReadings table.
        """
        values = []
        for row in self.data:
           values.append(f"('{row['id']}', '{row['nmi']}', '{row['timestamp']}', {row['consumption']})")
        values = ",".join(values)
        with open(self.output_file_path, 'a') as file:
            file.write(f"INSERT INTO meter_readings (id, nmi, timestamp, consumption) VALUES {values};\n")



def process_data(queue):
    """
    Worker function to process data from the queue.
    Each worker will parse a block of data and insert it into the database.
    """
     # Initialize the data parser
    while True:
        block = queue.get()  # Get a block from the queue
        if block is None:  # If None, exit the loop
            break
        
        # Parse the block
        parser = DataParser(block)
        parsed_rows = parser.parse()
        
        # Create the Insert Values script
        create_script = DDLScriptCreatorForMeterReadings(parsed_rows, OUTPUT_FILE_PATH)
        create_script.insert_values_script()

def read_file_in_chunks(file_path, queue): 
    """
    Reads the file in chunks and sends each block to the queue.
    """
    with open(file_path, 'r') as file:
        block = []  # List to accumulate the lines of a block
        for line in file:
            if line.startswith('200'):  # Start of a new block
                if block:  # If there's an existing block, push it for processing
                    queue.put(block)
                    block = []  # Reset the block
                block = [line]  # Start a new block with the current line
            elif line.startswith('300'):  # Inside the data block
                block.append(line)
        
        # At the end of the file, if there's an unfinished block, send it
        if block:
            queue.put(block)

    # Send sentinel value to workers to stop
    for _ in range(os.cpu_count()):
        queue.put(None)
    

def run(file_path):
    """
    Main function to run the data ingestion process.
    """
    
    queue = Queue(maxsize=50)  # Create a Queue with max size to control data flow

    # Start worker processes
    workers = []
    for _ in range(cpu_count()):
        p = Process(target=process_data, args=(queue,))
        p.start()  # Start worker processes
        workers.append(p)

    # Start streaming blocks into the queue (before workers start pulling data)
    read_file_in_chunks(file_path, queue)  # Start reading and sending blocks to the queue

    # Wait for all workers to finish processing
    for p in workers:
        p.join()  # Ensure all worker processes have completed

    print(f"Processing complete. Output written to {OUTPUT_FILE_PATH}")