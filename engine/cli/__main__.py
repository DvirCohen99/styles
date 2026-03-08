"""Allow running CLI as: python -m engine.cli"""
from engine.cli.commands import cli

if __name__ == "__main__":
    cli()
