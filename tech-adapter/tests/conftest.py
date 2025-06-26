from dotenv import load_dotenv

load_dotenv("tests/fixtures/test.environment", override=True)

from src import settings  # noqa
