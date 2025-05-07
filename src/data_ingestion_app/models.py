from django.db import models
import uuid

# Create your models here.

class MeterReadings(models.Model):
    """
    Model to store meter readings.
    """
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    nmi = models.CharField(max_length=10, null=False, blank=False)
    timestamp = models.DateTimeField(null=False, blank=False)
    consumption = models.DecimalField(max_digits=10, decimal_places=2, null=False, blank=False)
    
    class Meta:
        db_table = 'meter_readings'
        unique_together = ('nmi', 'timestamp')
   
