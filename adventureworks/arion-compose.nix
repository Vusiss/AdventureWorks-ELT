{ pkgs, ... }:
{
  project.name = "sqlserver-project";

  services.sqlserver = {
    service.image = "mcr.microsoft.com/mssql/server:2019-latest"; 
    
    service.environment = {
      ACCEPT_EULA = "Y";
      # WARNING: SQL Server enforces strict passwords! 
      # It MUST be 8+ chars, with uppercase, lowercase, and numbers/symbols.
      MSSQL_SA_PASSWORD = "SuperSecretPassword123!"; 
    };
    
    service.ports = [
      "1433:1433"
    ];
    
    service.volumes = [
      # Mount the backup folder into the container so SQL Server can see the .bak file
      "${builtins.toString ./.}/backup:/backup"
      
      # Optional: Persist the actual database files across restarts
      "sqldata:/var/opt/mssql"
    ];

    
  };


  docker-compose.volumes = {
    sqldata = {};
  };
}