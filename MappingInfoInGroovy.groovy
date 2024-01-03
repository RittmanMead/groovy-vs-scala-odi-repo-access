import oracle.odi.core.config.MasterRepositoryDbInfo
import oracle.odi.core.config.PoolingAttributes
import oracle.odi.core.config.WorkRepositoryDbInfo
import oracle.odi.core.OdiInstance
import oracle.odi.core.config.OdiInstanceConfig
import oracle.odi.core.security.Authentication
import oracle.odi.domain.project.OdiProject
import oracle.odi.domain.project.OdiFolder
import oracle.odi.domain.mapping.IMapComponent
import oracle.odi.domain.mapping.Mapping
import oracle.odi.domain.project.finder.IOdiProjectFinder
import oracle.odi.domain.mapping.finder.IMappingFinder
import oracle.odi.domain.mapping.component.ExpressionComponent
import oracle.odi.domain.mapping.component.FilterComponent



// access and search parameters
Map<String,String> odiAccessConfig = [
        masterRepositoryJdbcUrl: "jdbc:oracle:thin:@192.168.1.47:1521:ORCL",
        masterRepositoryJdbcDriver: "oracle.jdbc.OracleDriver",
        masterRepositoryJdbcUser: "prod_odi_repo",
        masterRepositoryJdbcPassword: "oracle",
        workRepositoryName: "WORKREP",
        odiUserName: "SUPERVISOR",
        odiUserPassword: "SUPERVISOR"
]

Map<String,String> mappingSearch = [
        projectCode: "DEMO",
        folderName: "SQL_to_ODI",
        mappingName: "SQL_to_ODI___Expressions_3"
]
// maps in Groovy can be accessed with the mapName.attributeName notation but it is unsafe - errors will pop up at runtime.
// could use singleton objects here. they are not quite as elegant as Scala's Objects.
/*
def mySingletonObject = {
  def property = "Singleton Property"

  def myFunction() {
    println("Function called in singleton object")
  }

  // Return the object with properties and functions
  [property: property, myFunction: myFunction]
}()

// Accessing properties and calling functions in the singleton object
println(mySingletonObject.property)
mySingletonObject.myFunction()
*/


// ODI Instance (not required in Groovy if we decide to run this code from ODI Studio)
OdiInstance getOdiInstance(odiAccessConfig) {
  MasterRepositoryDbInfo masterRepoDbInfo = new MasterRepositoryDbInfo(
          odiAccessConfig.masterRepositoryJdbcUrl,
          odiAccessConfig.masterRepositoryJdbcDriver,
          odiAccessConfig.masterRepositoryJdbcUser,
          odiAccessConfig.masterRepositoryJdbcPassword.toCharArray(),
          new PoolingAttributes()
  )

  WorkRepositoryDbInfo workRepoDbInfo = new WorkRepositoryDbInfo(
          odiAccessConfig.workRepositoryName,
          new PoolingAttributes()
  )

  OdiInstance instance = OdiInstance.createInstance(new OdiInstanceConfig(masterRepoDbInfo, workRepoDbInfo))

  Authentication auth  = instance.getSecurityManager().createAuthentication(
          odiAccessConfig.odiUserName,
          odiAccessConfig.odiUserPassword.toCharArray()
  )

  instance.getSecurityManager().setCurrentThreadAuthentication(auth)

  instance
}

OdiInstance odiInstance = getOdiInstance(odiAccessConfig) // comment this line out to run the code from ODI Studio

// finders
IOdiProjectFinder projectFinder = (IOdiProjectFinder)odiInstance.getTransactionalEntityManager().getFinder(OdiProject.class)
IMappingFinder mappingFinder = (IMappingFinder)odiInstance.getTransactionalEntityManager().getFinder(Mapping.class)

// analyse mapping
List<String> analyseMapping(projectFinder, mappingFinder, mappingSearch) {

  OdiProject foundProject = projectFinder.findByCode(mappingSearch.projectCode)

  if (foundProject) {
    OdiFolder foundFolder = foundProject.getFolders().find { OdiFolder f -> f.name == mappingSearch.folderName }

    if (foundFolder) {
      Mapping foundMapping = mappingFinder.findByName(foundFolder, mappingSearch.mappingName)

      if (foundMapping) {
        List<IMapComponent> mappingComponentsToAnalyse = foundMapping.getAllComponents()

        List<String> mappingComponents = mappingComponentsToAnalyse.collect { IMapComponent comp ->
          if (comp instanceof FilterComponent) {
            "Filter Component ${((FilterComponent)comp).name} has filtering expression ${((FilterComponent)comp).filterConditionText}."
          } else if (comp instanceof ExpressionComponent) {
            "Expression Component ${((ExpressionComponent)comp).name} has ${((ExpressionComponent)comp).attributes.size} attributes."
          } else {
            "Mapping Component ${comp.name} is of type ${comp.typeName}."
          }
        }

        return mappingComponents
      } else throw new Exception("Error: Mapping named '${mappingSearch.mappingName}' not found in Folder '${mappingSearch.folderName}'.")
    } else throw new Exception("Error: Folder named '${mappingSearch.folderName}' not found in Project '${mappingSearch.projectCode}'.")
  } else throw new Exception("Error: Project with code '${mappingSearch.projectCode}' not found.")
}

// output and exit
try {
  List<String> mappingAnalysis = analyseMapping(projectFinder, mappingFinder, mappingSearch)
  println("-= ${mappingSearch.mappingName} Mapping Components =-\n\t${mappingAnalysis.join('\n\t')} ")
  System.exit(0)
} catch (Exception e) {
  println(e.message)
  System.exit(1)
}
