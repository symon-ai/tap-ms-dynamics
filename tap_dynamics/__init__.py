import json
import sys
import traceback
import singer
from singer import utils

from tap_dynamics.discover import discover
from tap_dynamics.sync import sync
from tap_dynamics.symon_exception import SymonException

REQUIRED_CONFIG_KEYS = [
    "start_date",
    "organization_uri",
    "user_agent",
    "client_id",
    "client_secret",
    "redirect_uri",
    "refresh_token",
    "object"
]
LOGGER = singer.get_logger()

# for symon error logging
ERROR_START_MARKER = '[tap_error_start]'
ERROR_END_MARKER = '[tap_error_end]'


@utils.handle_top_exception(LOGGER)
def main():
    try:
        # used for storing error info to write if error occurs
        error_info = None
        # Parse command line arguments
        args = utils.parse_args(REQUIRED_CONFIG_KEYS)

        # If discover flag was passed, run discovery mode and dump output to stdout
        if args.discover:
            catalog = discover(args.config, args.config_path)
            catalog.dump()
        # Otherwise run in sync mode
        else:
            if args.catalog:
                catalog = args.catalog
            else:
                catalog = discover(args.config, args.config_path)
            sync(args.config, args.config_path, args.state, catalog)
    except SymonException as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        error_info = {
            'message': traceback.format_exception_only(exc_type, exc_value)[-1],
            'code': e.code,
            'traceback': "".join(traceback.format_tb(exc_traceback))
        }

        if e.details is not None:
            error_info['details'] = e.details
        raise
    except BaseException as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        error_info = {
            'message': traceback.format_exception_only(exc_type, exc_value)[-1],
            'traceback': "".join(traceback.format_tb(exc_traceback))
        }
        raise
    finally:
        if error_info is not None:
            try:
                error_file_path = args.config.get('error_file_path', None)
                if error_file_path is not None:
                    try:
                        with open(error_file_path, 'w', encoding='utf-8') as fp:
                            json.dump(error_info, fp)
                    except:
                        pass
                # log error info as well in case file is corrupted
                error_info_json = json.dumps(error_info)
                error_start_marker = args.config.get('error_start_marker', ERROR_START_MARKER)
                error_end_marker = args.config.get('error_end_marker', ERROR_END_MARKER)
                LOGGER.info(f'{error_start_marker}{error_info_json}{error_end_marker}')
            except:
                # error occurred before args was parsed correctly, log the error
                error_info_json = json.dumps(error_info)
                LOGGER.info(f'{ERROR_START_MARKER}{error_info_json}{ERROR_END_MARKER}')


if __name__ == "__main__":
    main()
