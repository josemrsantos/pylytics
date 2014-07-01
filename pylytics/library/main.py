import logging
import sys

import argparse
import importlib
import os

from utils.text_conversion import underscore_to_camelcase
from utils.terminal import print_status


def all_facts():
    """Return the names of all facts in the facts folder."""
    fact_filenames = os.listdir('fact')
    facts = []
    for filename in fact_filenames:
        if filename.startswith('fact_') and filename.endswith('.py'):
            facts.append(filename.split('.')[0])
    return facts


def get_class(module_name, dimension=False, package=None):
    """
    Effectively does this:
    from fact/fact_count_all_the_sales import FactCountAllTheSales
    return FactCountAllTheSales

    Example usage:
    get_class('fact_count_all_the_sales')

    If dimension is True then it searches for a dimension instead.

    """
    module_name_parts = []
    if package:
        module_name_parts.append(package)
    module_name_parts.append("dim" if dimension else "fact")
    module_name_parts.append(module_name)
    qualified_module_name = ".".join(module_name_parts)

    module = importlib.import_module(qualified_module_name)
    class_name = underscore_to_camelcase(module_name)
    my_class = getattr(module, class_name)
    return my_class


def print_summary(errors):
    """Print out a summary of the errors which happened during run_command."""
    print_status("Summary", format='reverse', space=True, timestamp=False,
                 indent=False)
    if len(errors) == 0:
        print_status("Everything went fine!", format='green', timestamp=False,
                     indent=False)
    else:
        print_status("{0} commands not executed: {1}".format(
                len(errors),
                ", ".join(errors.keys()),
                ),
            timestamp=False,
            indent=False
            )
        items = [
            "- {}: {} {}".format(key, type(value).__name__,
                                 value) for key, value in errors.items()]

        print_status("\n".join(items), timestamp=False, indent=False)


def _process_scripts(scripts):
    """
    Looks for the scripts in the folder 'scripts' at the root of the
    pylytics project, and runs them.

    """
    print_status('Running scripts.')
    for script in scripts:
        try:
            script = importlib.import_module('scripts.{}'.format(script))
        except ImportError as exception:
            print_status('Unable to import the script `{}`. '
                         'Traceback - {}.'.format(script, str(exception)))
            continue

        print_status('Running {}'.format(script))

        try:
            script.main()
        except Exception, e:
            print_status(repr(e))


def _extract_scripts(command, fact_classes, script_type='setup_scripts'):
    """
    Introspects the list of fact_classes and returns a list of script names
    that need to be run.

    If the same script is listed in multiple fact classes, then it will
    only appear once in the list.

    """
    scripts = []
    for fact_class in fact_classes:

        try:
            script_dict = getattr(fact_class, script_type)
        except AttributeError:
            print_status('Unable to find {} in {}.'.format(
                    script_type, fact_class.__class__.__name__))
            continue

        if isinstance(script_dict, dict):
            if command in script_dict:
                scripts.extend(script_dict[command])
            else:
                print_status("No {} found for {}.".format(
                        script_type, fact_class.__class__.__name__))
        else:
            print_status('Setup_scripts must be a dictionary - ignoring.')

    # Remove duplicates.
    return list(set(scripts))


def run_command(db_name, facts, command):
    """
    Run command for each fact in facts.

    """
    from connection import DB
    errors = {}

    with DB(db_name) as database_connection:

        # Get all the fact classes.
        fact_classes = []
        for fact in facts:
            try:
                FactClass = get_class(fact)(connection=database_connection)
            except Exception as error:
                # Inline import as we're not making log object global.
                ##
                log = logging.getLogger("pylytics")
                log.error("Unable to load fact '{}' due to {}: "
                          "{}".format(fact, error.__class__.__name__, error))
            else:
                fact_classes.append(FactClass)

        # Execute any setup scripts that need to be run.
        print_status("Checking setup scripts.", indent=False, space=True,
                     format='blue', timestamp=False)
        setup_scripts = _extract_scripts(command, fact_classes)
        if setup_scripts:
            _process_scripts(setup_scripts)
        else:
            print_status('No setup scripts to run.')

        # Execute the command on each fact class.
        for fact_class in fact_classes:
            fact_class_name = fact_class.__class__.__name__
            print_status("Running {0} {1}".format(
                    fact_class_name, command),
                    format='blue', indent=False, space=True, timestamp=False)
            try:
                getattr(fact_class, command)()
            except Exception as e:
                print_status("Running {0} {1} failed!".format(fact_class_name,
                        command), format='red')
                errors['.'.join([fact_class_name, command])] = e

        # Execute any exit scripts that need to be run.
        print_status("Checking exit scripts.", indent=False, space=True,
                     format='blue', timestamp=False)
        exit_scripts = _extract_scripts(command, fact_classes,
                                        script_type='exit_scripts')
        if exit_scripts:
            _process_scripts(exit_scripts)
        else:
            print_status('No exit scripts to run.')

    print_summary(errors)


def main():
    """ Main function called by the manage.py from the project directory.
    """
    from fact import Fact

    # Enable log output before loading settings so we have visibility
    # of any load errors.
    log = logging.getLogger("pylytics")
    log.addHandler(logging.StreamHandler(sys.stdout))
    log.setLevel(logging.INFO)

    from pylytics.library.settings import Settings, settings

    try:
        sentry_dsn = settings.sentry_dsn
    except AttributeError:
        # No Sentry DSN configured. As you were.
        pass
    else:
        # Only import raven if we're actually going to use it.
        from raven.handlers.logging import SentryHandler
        log.addHandler(SentryHandler(sentry_dsn))

    parser = argparse.ArgumentParser(
        description = "Run fact scripts.")
    parser.add_argument(
        'fact',
        choices = ['all'] + all_facts(),
        help = 'The name(s) of the fact(s) to run e.g. fact_example.',
        nargs = '+',
        type = str,
        )
    parser.add_argument(
        'command',
        choices = Fact.public_methods(),
        help = 'The command you want to run.',
        nargs = 1,
        type = str,
        )
    parser.add_argument(
        '--settings',
        help = 'The path to the settings module e.g /etc/foo/bar',
        type = str,
        nargs = 1,
        )

    args = parser.parse_args().__dict__
    facts = set(args['fact'])
    command = args['command'][0]

    if 'all' in facts:
        print_status('Running all fact scripts:', indent=False,
                     timestamp=False, format='reverse', space=True)
        facts = all_facts()

    # Prepend an extra settings file if one is specified.
    settings_module = args["settings"]
    if settings_module:
        settings.prepend(Settings.load(settings_module))

    run_command(settings.pylytics_db, facts, command)
