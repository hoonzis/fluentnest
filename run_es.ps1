Install-ChocolateyZipPackage 'elastics' 'http://download.elasticsearch.org/elasticsearch/elasticsearch/elasticsearch-1.5.1.zip' "$(Split-Path -parent $MyInvocation.MyCommand.Definition)"
 
$path = "$(Split-Path -parent $MyInvocation.MyCommand.Definition)\elasticsearch-1.5.1\bin"
 
Install-ChocolateyPath $path
 
#$java_home = [Environment]::GetEnvironmentVariable("JAVA_HOME", "Machine")
#[Environment]::SetEnvironmentVariable("JAVA_HOME", $java_home)
 
[Environment]::SetEnvironmentVariable("ES_START_TYPE", "auto")
[Environment]::SetEnvironmentVariable("ES_START_TYPE", "auto")
[Environment]::SetEnvironmentVariable("ELASTIC_SEARCH_PATH", "$(Split-Path -parent $MyInvocation.MyCommand.Definition)\elasticsearch-1.5.1\", "Machine")
 
#if ([Environment]::GetEnvironmentVariable("DATA_DIR") -eq $null ) { throw "DATA_DIR Must be set" }
#if ([Environment]::GetEnvironmentVariable("LOG_DIR") -eq $null ) { throw "LOG_DIR Must be set" }
#if ([Environment]::GetEnvironmentVariable("CONF_DIR") -eq $null ) { throw "CONF_DIR Must be set" }
#if ([Environment]::GetEnvironmentVariable("CONF_FILE") -eq $null ) { throw "CONF_FILE Must be set" }
 
&"$path\service.bat" "install"
 
&"$path\service.bat" "start"