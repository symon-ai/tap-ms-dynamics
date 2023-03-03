import singer
from singer import Transformer, metadata

from tap_dynamics.streams import get_streams

LOGGER = singer.get_logger()

def sync(config, config_path, state, catalog):
    """ Sync data from tap source """

    streams = get_streams(config, config_path)
    # TODO: document that newly created fields won't be selected as currently implemented

    LOGGER.info('There are {:d} valid streams in MS Dynamics'.format(len(streams)))

    with Transformer() as transformer:
        for stream in catalog.get_selected_streams(state):
            tap_stream_id = stream.tap_stream_id
            stream_obj = streams[tap_stream_id]
            stream_schema = stream.schema.to_dict()
            stream_metadata = metadata.to_map(stream.metadata)

            # drop unsupported or unselected columns
            desired_columns = [colname for colname in stream_schema['properties'] if should_sync_column(stream_metadata, colname)]

            LOGGER.info('Starting sync for stream: %s', tap_stream_id)

            state = singer.set_currently_syncing(state, tap_stream_id)
            singer.write_state(state)

            singer.write_schema(
                tap_stream_id,
                stream_schema,
                stream_obj.key_properties,
                stream.replication_key
            )

            state = stream_obj.sync(state, stream_schema, stream_metadata, config, transformer, desired_columns)
            singer.write_state(state)

    state = singer.set_currently_syncing(state, None)
    singer.write_state(state)


def should_sync_column(metadata, field_name):
    field_metadata = metadata.get(('properties', field_name), {})
    return singer.should_sync_field(field_metadata.get('inclusion'),
                                    field_metadata.get('selected'),
                                    True)
