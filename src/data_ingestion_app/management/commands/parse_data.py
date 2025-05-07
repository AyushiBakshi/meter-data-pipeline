from django.core.management.base import BaseCommand
import os
from data_ingestion_app.services import run

class Command(BaseCommand):
    help = 'Parse data command'

    def add_arguments(self, parser):
        parser.add_argument('--file', type=str, help='Path to the NEM12 data file')


    def handle(self, *args, **kwargs):
        file_path = kwargs['file']
        if not file_path:
            self.stderr.write("Error: --file argument is required")
            return
        if not os.path.exists(file_path):
            self.stderr.write(f"Error: File {file_path} does not exist")
            return
        self.stdout.write(f"Processing file: {file_path}")
        run(file_path)