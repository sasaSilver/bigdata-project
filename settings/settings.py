from pydantic import BaseModel
from pydantic_settings import BaseSettings, SettingsConfigDict

class Postgres(BaseModel):
    host: str
    port: str
    user: str
    password: str
    dbname: str

    def conn_string(self, async_: bool = False):
        driver_ext = '+psycopg' if async_ else ''
        return f"postgresql{driver_ext}://{self.user}:{self.password}@{self.host}:{self.port}/{self.dbname}"

class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file='.env',
        env_nested_delimiter='__',
    )
    pg: Postgres
