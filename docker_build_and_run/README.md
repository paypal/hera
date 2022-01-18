#Building as HeraBox
This folder helps in building
1. hera
2. HeraMock
3. heraclient jar
4. Hera integrated sample Raptor Application

`./build.sh
Parameters that controls build
BUILD_HERA - default to true
BUILD_HERA_MOCK - default to true
BUILD_SAMPLE_APP - default to true (requirement java)
`
#Running as HeraBox
The docker files helps to run MySQL, Hera, HeraMock and TestApplication

1. ./start.sh - should be run after building
2. Parameters
   1. MYSQL_VERSION - Default is 'latest'
   2. HERA_DB_ROOT_PASSWORD - Default is 'UseHeraToScaleDB'
   3. HERA_DB_SCHEMA - Default is 'testschema'
   4. HERA_DB_USER - Default is 'herauser'
   5. HERA_DB_PASSWORD - Default is 'herapassword'
   6. HERA_DISABLE_SSL - Default is 'false'
   7. HERA_RUN_WITH_MOCK - Default is 'true'
   8. HERA_TIME_ZONE - Default is 'America/Los_Angeles'"