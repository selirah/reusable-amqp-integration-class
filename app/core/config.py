from pydantic_settings import BaseSettings
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())


class Settings(BaseSettings):
    AMQP_HOST: str
    AMQP_PORT: str
    AMQP_USERNAME: str
    AMQP_PASSWORD: str
    AMQP_VHOST: str

settings = Settings()