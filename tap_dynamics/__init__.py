import json
import os
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


def resolve_safe_error_file_path(error_file_path, base_dir):
    """Validate a config-supplied error file path against path manipulation (CWE-73).

    The platform sets ``error_file_path`` to a known, controlled location, but Veracode
    treats it as tainted input. This canonicalizes the requested path and confirms it is
    contained within ``base_dir`` (an allowed root), rejecting absolute-escape and ``..``
    traversal. Returns the canonical, in-bounds path, or ``None`` when the path is missing
    or fails validation (in which case callers fall back to marker-based error logging).
    """
    if not error_file_path:
        return None

    allowed_root = os.path.realpath(base_dir)
    resolved_path = os.path.realpath(os.path.join(allowed_root, error_file_path))

    try:
        # commonpath raises ValueError on mixed drives/absolute-vs-relative; both are unsafe.
        if os.path.commonpath([allowed_root, resolved_path]) != allowed_root:
            return None
    except ValueError:
        return None

    return resolved_path


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
                # Validate the config-supplied path against path manipulation (CWE-73)
                # before writing; reject traversal / out-of-bounds paths and fall back to
                # the marker-based error logging below.
                error_file_base_dir = args.config.get('error_file_base_dir', os.getcwd())
                safe_error_file_path = resolve_safe_error_file_path(error_file_path, error_file_base_dir)
                if safe_error_file_path is not None:
                    try:
                        with open(safe_error_file_path, 'w', encoding='utf-8') as fp:
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
