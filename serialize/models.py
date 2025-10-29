from django.db import models


# Create your models here.
class SerializeMaster(models.Model):
    part_id = models.CharField(max_length=255, db_index=True, unique=True)
    configuration_id = models.IntegerField()
    process_step_num = models.IntegerField()
    job_number = models.CharField(max_length=255, db_index=False)
    start_operator = models.CharField(max_length=255, db_index=False)
    start_datetime = models.DateTimeField(null=True, blank=True)
    end_operator = models.CharField(max_length=255, db_index=False)
    end_datetime = models.DateTimeField(null=True, blank=True)
    notes = models.CharField(max_length=255, db_index=False)
    scrapped = models.CharField(max_length=255, db_index=False)
    created_by = models.CharField(max_length=255, db_index=False)
    created_datetime = models.DateTimeField(null=True, blank=True)
    modified_by = models.CharField(max_length=255, db_index=False)
    modified_datetime = models.DateTimeField(null=True, blank=True)

    def __str__(self):
        return self.part_id
