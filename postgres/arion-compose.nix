{ pkgs, ... }:
{
  project.name = "postgres-project";

  services.postgres = {
    service.image = "postgres:15"; 
    
    service.environment = {
      POSTGRES_USER = "postgres";
      POSTGRES_PASSWORD = "mysecretpassword";
      POSTGRES_DB = "company_db"; 
    };
    
    service.ports = [
      "5432:5432" # 5432 is the standard Postgres port
    ];
    
    service.volumes = [
      # Mount our init-data folder. Postgres will run everything inside it automatically!
      "${builtins.toString ./.}/init-data:/docker-entrypoint-initdb.d"
      "pgdata:/var/lib/postgresql/data"
    ];
  };

  # Declare the permanent hard drive volume for the data
  docker-compose.volumes = {
    pgdata = {};
  };
}