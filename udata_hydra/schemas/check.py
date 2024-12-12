import json

from marshmallow import Schema, fields


class CheckSchema(Schema):
    check_id = fields.Integer(data_key="id")
    catalog_id = fields.Integer()
    url = fields.Str()
    domain = fields.Str()
    created_at = fields.DateTime()
    check_status = fields.Integer(data_key="status")
    headers = fields.Function(lambda obj: json.loads(obj["headers"]) if obj["headers"] else {})
    timeout = fields.Boolean()
    response_time = fields.Float()
    error = fields.Str()
    dataset_id = fields.Str()
    resource_id = fields.UUID()
    next_check_at = fields.DateTime()
    deleted = fields.Boolean()
    parsing_started_at = fields.DateTime()
    parsing_finished_at = fields.DateTime()
    parsing_error = fields.Str()
    parsing_table = fields.Str()
    parquet_url = fields.Str()
    parquet_size = fields.Integer()

    def create(self, data):
        return self.load(data)


class CheckGroupBy(Schema):
    value = fields.Str()
    count = fields.Integer()
