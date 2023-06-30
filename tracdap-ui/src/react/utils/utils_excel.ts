//import XLSX from "@sheet/image";
//import {utils, writeFile} from "xlsx";
import {TableRow} from "../../types/types_general";
// import {Images, TracClassification} from "../../config/config";
// import {resizeBase64Image, setDownloadName} from "./utils";
import {tracdap as trac} from "@finos/tracdap-web-api";
import {setDownloadName} from "./utils_downloads";
import {createTableData} from "./utils_attributes_and_parameters";

/**
 * A function that takes a dataset and converts it to an XLSX and initiates a download. The data is stored in one tab
 * and the schema in another. This is a simple download because it uses the open source xslx plugin. This is free, but
 * there is a paid version to that allows the Excel file to be formatted. This simple version is a fallback in case the
 * paid for version is not available or an un-formatted export is needed.
 * @param data - The data to export.
 * @param schema - The schema for the dataset.
 * @param tag - The TRAC metadata for the object.
 * @param userId - The user ID of the person exporting the dataset, used in the Excel filename and the metadata
 * tab.
 * @param userName - The username of the person exporting the dataset, used in the metadata tab.
 * @param tenant - The tenant that the dataset was exported from.
 */
// TODO would be good to have a way of working out the max char length of each column and set the wch parameter to autofit this
export const downloadDataAsXlsxSimple = (data: TableRow[], schema: trac.metadata.IFieldSchema[], tag: undefined | trac.metadata.ITag, userId: string, userName: string, tenant?: string,): void => {

    const utils : Record<string, any>= {}

    // Create a new workbook
    const wb = utils.book_new()

    // Add some workbook metadata
    wb.Props = {
        Title: "TRAC dataset export",
        CreatedDate: new Date(),
        Author: userId
    }

    // Create a sheet from the rowData
    let ws0 = utils.json_to_sheet(data)
    let ws1 = utils.json_to_sheet(schema)

    // Create a metadata sheet, this will have multiple tables in it, so we add a header for each table

    // First is the metadata
    const exportMetadata = [["Exported by ", `${userName} (${userId})`], ["Tenant ", tenant]]
    const attrsMetadata = createTableData(tag, [{key: "attrs"}], {})
    const headerMetadata = createTableData(tag, [{key: "header"}], {})

    // Initiate the sheet and add the titles
    let ws2 = utils.aoa_to_sheet([["Export information:"]])
    utils.sheet_add_aoa(ws2, [["Header metadata:"]], {origin: `A6`})
    utils.sheet_add_aoa(ws2, [["Attributes metadata:"]], {origin: `A${9 + headerMetadata[0].data.length}`})

    // Now add in the data
    utils.sheet_add_aoa(ws2, exportMetadata, {origin: "A3"})

    utils.sheet_add_json(ws2, headerMetadata[0].data, {
        origin: "A8",
        skipHeader: true
    })

    utils.sheet_add_json(ws2, attrsMetadata[0].data, {
        origin: `A${11 + headerMetadata[0].data.length}`,
        skipHeader: true
    })

    // Put the worksheets into the workbook
    utils.book_append_sheet(wb, ws0, "Data")
    utils.book_append_sheet(wb, ws1, "Schema")
    utils.book_append_sheet(wb, ws2, "Metadata")

    // Trigger the download
    // writeFile(wb, setDownloadName(tag, "xlsx", true, userId), {cellStyles: true})
}