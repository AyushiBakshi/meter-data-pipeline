import datetime
import os, uuid
from django.test import TestCase

from data_ingestion_app.services import DDLScriptCreatorForMeterReadings, DataParser    
from data_ingestion_app.models import MeterReadings


class MeterReadingsTestCase(TestCase):
    def setUp(self):
        self.meter_readings = MeterReadings.objects.create(
            id = uuid.uuid4(),
            nmi="1234567890",
            timestamp="2023-10-01T00:00:00Z",
            consumption=100.50
        )
    
    def test_meter_readings_creation(self):
        self.assertEqual(self.meter_readings.nmi, "1234567890")
        self.assertEqual(self.meter_readings.timestamp, "2023-10-01T00:00:00Z")
        self.assertEqual(self.meter_readings.consumption, 100.50)
    
    def test_meter_readings_does_not_exist(self):
        self.assertIsNone(MeterReadings.objects.filter(nmi="non_existent_nmi").first())
    
class DataParserTestCase(TestCase):
    def setUp(self):
        self.block = []
        self.block.append("200,NEM1201009,E1E2,1,E1,N1,01009,kWh,30,20050610\n")
        self.block.append("300,20050301,0,0,0,0,0,0,0,0,0,0,0,0,0.461,0.810,0.568,1.234,1.353,1.507,1.344,1.773,0.848,1.271,0.895,1.327,1.013,1.793,0.988,0.985,0.876,0.555,0.760,0.938,0.566,0.512,0.970,0.760,0.731,0.615,0.886,0.531,0.774,0.712,0.598,0.670,0.587,0.657,0.345,0.231,A,,,20050310121004,20050310182204\n")
    
    def test_parse_data(self):
        parser = DataParser(block=self.block)
        data = parser.parse()
        self.assertEqual(data[0]['nmi'], "NEM1201009")
        self.assertEqual(data[0]['timestamp'], datetime.datetime.strptime("20050301", '%Y%m%d' ))
        self.assertEqual(data[0]['consumption'], 31.44)  
    


class DDLScriptCreatorForMeterReadingsTestCase(TestCase):
    def setUp(self):
        self.parsed_data = [
            {
                'id': str(uuid.uuid4()),
                'nmi': 'NEM1201009',
                'timestamp': datetime.datetime.strptime("20050301", '%Y%m%d'),
                'consumption': 31.44
            }
        ]
        self.output_file_path = "insert_script.sql"
        if os.path.exists(self.output_file_path):
            os.remove(self.output_file_path)
        
    
    def test_insert_values_script(self):
        creator = DDLScriptCreatorForMeterReadings(self.parsed_data, self.output_file_path)
        creator.insert_values_script()
        id = str(self.parsed_data[0]['id'])
        with open(self.output_file_path, 'r') as file:
            content = file.read()
            success_result = f"INSERT INTO meter_readings (id, nmi, timestamp, consumption) VALUES ('{id}', 'NEM1201009', '2005-03-01 00:00:00', 31.44);\n"
            self.assertIn(success_result, content)
            