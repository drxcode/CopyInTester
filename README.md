# DataRox CopyIn Performance Test Application

### Pre-requisites:
- PostgreSQL 9.x installed and configured (any instance, local or remote)
- .NET >=3.5 or higher
- Visual Studio >=2010
- Nuget Package Manager. See: http://docs.nuget.org/consume/installing-nuget

### To build and run the test application:
- Clone the source locally and open the solution file in Visual Studio
- Using Nuget, add npgsql and trove.nini package references
- Add the CopyIn trial DLL as a reference: http://www.datarox.co/downloads/DRX.Database.PostgreSQL.zip
- Build the application
- Edit the ini generated in the Release directory
- Run the output exe e.g from a DOS window

For more information see: http://www.datarox.co  
For any issues please contact: support @ datarox.co or use the github issues tracker
