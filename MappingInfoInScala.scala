import oracle.odi.core.config.MasterRepositoryDbInfo
import oracle.odi.core.config.PoolingAttributes
import oracle.odi.core.config.WorkRepositoryDbInfo
import oracle.odi.core.OdiInstance
import oracle.odi.core.config.OdiInstanceConfig
import oracle.odi.core.security.Authentication
import oracle.odi.domain.project.OdiProject
import oracle.odi.domain.project.OdiFolder
import oracle.odi.domain.mapping.{IMapComponent, Mapping}
import oracle.odi.domain.project.finder.IOdiProjectFinder
import oracle.odi.domain.mapping.finder.IMappingFinder
import oracle.odi.domain.mapping.component.{ExpressionComponent, FilterComponent}

import scala.jdk.CollectionConverters.*


object MappingInfoInScala extends App {

    // access and search parameters
    object odiAccessConfig {
        val masterRepositoryJdbcUrl: String = "jdbc:oracle:thin:@192.168.1.47:1521:ORCL"
        val masterRepositoryJdbcDriver: String = "oracle.jdbc.OracleDriver"
        val masterRepositoryJdbcUser: String = "prod_odi_repo"
        val masterRepositoryJdbcPassword: String = "oracle"
        val workRepositoryName: String = "WORKREP"
        val odiUserName: String = "SUPERVISOR"
        val odiUserPassword: String = "SUPERVISOR"
    }

    object mappingSearch {
        val projectCode: String = "DEMO"
        val folderName: String = "SQL_to_ODI"
        val mappingName: String = "SQL_to_ODI___Expressions_3"
    }

    // ODI instance
    lazy val odiInstance: OdiInstance = {
        val masterRepoDbInfo: MasterRepositoryDbInfo = new MasterRepositoryDbInfo(
            odiAccessConfig.masterRepositoryJdbcUrl,
            odiAccessConfig.masterRepositoryJdbcDriver,
            odiAccessConfig.masterRepositoryJdbcUser,
            odiAccessConfig.masterRepositoryJdbcPassword.toCharArray(),
            new PoolingAttributes()
        )

        val workRepoDbInfo: WorkRepositoryDbInfo = new WorkRepositoryDbInfo(
            odiAccessConfig.workRepositoryName,
            new PoolingAttributes()
        )

        val instance: OdiInstance = OdiInstance.createInstance(new OdiInstanceConfig(masterRepoDbInfo, workRepoDbInfo))

        val auth: Authentication = instance.getSecurityManager().createAuthentication(
            odiAccessConfig.odiUserName,
            odiAccessConfig.odiUserPassword.toCharArray()
        )

        instance.getSecurityManager().setCurrentThreadAuthentication(auth)

        instance
    }

    // finders
    lazy val projectFinder: IOdiProjectFinder = odiInstance.getTransactionalEntityManager().getFinder(classOf[OdiProject]).asInstanceOf[IOdiProjectFinder]
    lazy val mappingFinder: IMappingFinder = odiInstance.getTransactionalEntityManager().getFinder(classOf[Mapping]).asInstanceOf[IMappingFinder]


    val foundProject: Option[OdiProject] = Option(projectFinder.findByCode(mappingSearch.projectCode))

    val mappingAnalysis: Either[String, Vector[String]] = foundProject match {
        case None => Left(s"Error: Project with code '${mappingSearch.projectCode}' not found.")
        case Some(project) => {
            val foundFolder: Option[OdiFolder] = project.getFolders.asScala.toVector.find(_.getName == mappingSearch.folderName)

            foundFolder match {
                case None => Left(s"Error: Folder named '${mappingSearch.folderName}' not found in Project '${mappingSearch.projectCode}'.")
                case Some(folder) => {
                    val foundMapping: Option[Mapping] = Option(mappingFinder.findByName(folder, mappingSearch.mappingName))

                    foundMapping match {
                        case None => Left(s"Error: Mapping named '${mappingSearch.mappingName}' not found in Folder '${mappingSearch.folderName}'.")
                        case Some(mapping) => {
                            val mappingComponentsToAnalyse: Vector[IMapComponent] = mapping.getAllComponents.asScala.toVector
                            val mappingComponents: Vector[String] = mappingComponentsToAnalyse.map {
                                _ match {
                                    case filterComp: FilterComponent => s"Filter Component ${filterComp.getName} has filtering expression ${filterComp.getFilterConditionText}."
                                    case exprComp: ExpressionComponent => s"Expression Component ${exprComp.getName} has ${exprComp.getAttributes.size()} attributes."
                                    case otherComp => s"Mapping Component ${otherComp.getName} is of type ${otherComp.getTypeName}."
                                }
                            }
                            Right(mappingComponents)
                        }
                    }
                }
            }
        }
    }

    // output and exit
    mappingAnalysis match {
        case Left(errorMessage) => {
            println(errorMessage)
            sys.exit(1)
        }
        case Right(mappingComponents) => {
            println(s"-= ${mappingSearch.mappingName} Mapping Components =-\n\t${mappingComponents.mkString("\n\t")}")
            sys.exit(0)
        }
    }

}