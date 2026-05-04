from pydantic import BaseModel
from pydantic_settings import BaseSettings, SettingsConfigDict


class Postgres(BaseModel):
    host: str
    port: str
    user: str
    password: str
    dbname: str

    @property
    def conn_uri(self):
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.dbname}"

    @property
    def conn_dsn(self):
        return "host={host} port={port} user={user} password={password} dbname={dbname}".format(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            dbname=self.dbname,
        )


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_nested_delimiter="__",
        extra="ignore",
    )
    pg: Postgres
