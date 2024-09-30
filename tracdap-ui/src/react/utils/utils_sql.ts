//import alasql from "alasql";
import {DataRow} from "../../types/types_general";
import {getSmallDatasetByTag} from "./utils_trac_api";
import {isAppRunningLocally} from "./utils_general";
import {isObject} from "./utils_trac_type_chckers";
import {showToast} from "./utils_general";
import {tracdap as trac} from "@finos/tracdap-web-api";
import {wait} from "./utils_async";

/**
 * A function that routes a request to run an SQL query on a dataset (e.g. from the DataAnalyticsScene) to either a local execution
 * using a plugin called alaSql or to a server based instance of TRAC that pushed down the query to be executed. alaSql needs to
 * be installed via npm for this to work, however it triggers vulnerability warnings when scanned and is also not being maintained,
 * so we should never commit with it installed, and it should ONLY EVER be installed as a dev dependency. It is recommended to
 * only ever install it if you are debugging the UI after making changes to the relevant components.
 *
 * @param objectId - The object ID of the dataset to query.
 * @param header - The TRAC metadata header for the dataset.
 * @param tenant - The tenant that the user is on.
 * @param sqlQuery - The SQL query to run.
 * @returns The dataset from the query.
 */
export const decideWhichSqlToUse = async (objectId: string, header: trac.metadata.ITagHeader, tenant: string, sqlQuery: string): Promise<void | DataRow[]> => {

    if (isAppRunningLocally()) {

        console.log("LOG :: running SQL locally with AlaSql")
        // TOD move to streaming
        const {data} = await getSmallDatasetByTag<string, DataRow>(header, {tenant})

        return await executeAlaSql(data, sqlQuery)

    } else {

        console.log("LOG :: running SQL on server with Hive SQL")
        return await wait(500)
    }
}

/**
 * A function that executes an SQL query against a local dataset. This only works when on a local
 * environment. To run executions on other environments executeHiveSql needs to be used.
 *
 * @param dataset - The dataset to run the SQL on.
 * @param sqlQuery - The SQL query to run.
 * @returns The dataset from the query.
 */
export const executeAlaSql = async (dataset: DataRow[], sqlQuery: string): Promise<DataRow[]> => {

    // AlaSql uses slightly different syntax to HIVE, so we need to adapt for it here
    const alaSQlQuery = sqlQuery.replace("from dataset", "from ?")

    try {

        //return await alasql.promise(alaSQlQuery, [dataset])
        return []

    } catch (error) {

        const text = {
            title: "Failed to run the SQL",
            message: "The request to run the SQL did not complete successfully.",
            details: typeof error === "string" ? error : isObject(error) && typeof error.message === "string" ? error.message : undefined
        }

        showToast("error", text, "sql-error")

        if (typeof error === "string") {
            throw new Error(error)
        } else {
            console.error(error)
            throw new Error("Metadata download failed")
        }
    }
}