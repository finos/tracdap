/**
 * A group of utilities for handling file downloads in the UI.
 * @category Utils
 * @module DownloadUtils
 */

import {convertDateObjectToFormatCode} from "./utils_formats";
import {convertObjectTypeToString} from "./utils_trac_metadata";
import {downloadDataAsStream} from "./utils_trac_api";
import {hasOwnProperty} from "./utils_trac_type_chckers";
import Papa from "papaparse";
import type {TableRow} from "../../types/types_general";
import {tracdap as trac} from "@finos/tracdap-web-api";
import type {Value} from 'react-svg-pan-zoom';

/**
 * A function that simulates a download click so a file is downloaded.
 *
 * @param filename - The name of the file.
 * @param file - The data for the file.
 */
export const downloadFile = (filename: string, file: string): void => {

    let tempLink = document.createElement("a")
    tempLink.href = file
    tempLink.setAttribute("download", filename)

    // revoke the URL to prevent memory leaks
    // https://developer.chrome.com/en/articles/browser-fs-access/#:~:text=Working%20with%20system%20files%20technically%20works%20on%20all%20modern%20browsers.
    tempLink.addEventListener('click', () => {
        setTimeout(() => URL.revokeObjectURL(tempLink.href), 30 * 1000);
    });

    tempLink.click()
}

/**
 * A function that runs when the user wants to download a file from the application. It sets the file name
 * for the download using metadata from the object and the user.
 *
 * @param tag - The tag object or the TRAC metadata for the object being downloaded. In some cases this is
 * spoofed, for example when downloading a set of search results.
 * @param fileType - The fle type to use as the extension of the file e.g. csv. Can be blank if the
 * plugin performing the export automatically adds its own file extension.
 * @param addDate - Whether to add the datetime of the download to the filename.
 * @param userId - The user ID of the person downloading the file.
 * @returns - The filename.
 */
export const setDownloadName = (tag: undefined | trac.metadata.ITag, fileType: string, addDate: boolean, userId: string) => {

    // Set a date to add
    const date = addDate ? `_${convertDateObjectToFormatCode(new Date(), "FILENAME")}` : ""

    const objectKeyFromAttrs = tag?.attrs?.key?.stringValue || "object"
    const objectTypeAsString = tag?.header?.objectType ? convertObjectTypeToString(tag.header.objectType, true) : ""
    const objectId = tag?.header?.objectId || ""
    const objectVersion = hasOwnProperty(tag?.header, "objectVersion") ? `_object_version_v${tag?.header.objectVersion}` : ""
    const tagVersion = hasOwnProperty(tag?.header, "tagVersion") ? `_tag_version_v${tag?.header.tagVersion}` : ""

    // The check on filetype is needed because some plugins like HighCharts add their own extension so the '.' is not needed
    return `${objectTypeAsString}_${objectKeyFromAttrs}_${objectId}${objectVersion}${tagVersion}_${userId.toLowerCase()}${date}${fileType && fileType.length > 0 ? "." : ""}${fileType}`
}

/**
 * A function that takes a javascript object and converts it to a JSON object and automatically
 * initiates a download.
 *
 * @param tag - The TRAC metadata for the object.
 * @param userId - The user ID of the person using the application, this is included in the download file name.
 */
export const makeJsonDownload = (tag: trac.metadata.ITag, userId: string): void => {

    const json = JSON.stringify(tag)
    const blob = new Blob([json], {type: "application/json"})
    const filename = setDownloadName(tag, "json", true, userId)

    const jsonUrl = window.URL.createObjectURL(blob)

    downloadFile(filename, jsonUrl)
}

/**
 * A function that takes a JSON dataset held in memory and converts it to a csv file for downloading. This triggers the
 * downloading of the file.
 *
 * @param data - The data to convert to a csv.
 * @param tag - The metadata tag for the data, used to set the download name.
 * @param userId - The user ID requesting the download, used to set the download name.
 */
export const downloadDataAsCsvFromJson = (data: string | TableRow[], tag: undefined | trac.metadata.ITag, userId: string): void => {

    const csvString = typeof data === "string" ? data : Papa.unparse(data)
    const blob = new Blob([csvString], {type: 'text/csv;charset=utf-8;'})
    const filename = setDownloadName(tag, "csv", true, userId)

    const dataURL = window.URL.createObjectURL(blob)

    downloadFile(filename, dataURL)
}

/**
 * A function that takes streams a dataset in TRAC to the browser and converts it to a csv file for downloading. This triggers
 * the downloading of the file.
 *
 * @param tag - The metadata tag for the data, used to fetch the data and set the download name.
 * @param tenant - The name of the tenant that the data is stored in.
 * @param userId - The user ID requesting the download, used to set the download name.
 */
export const downloadDataAsCsvFromStream = async (tag: trac.metadata.ITag, tenant: string, userId: string): Promise<void> => {

    if (tag.header == null) return

    const {content: csvUint8Array} = await downloadDataAsStream({format: "text/csv", tagSelector: tag.header, tenant})

    const filename = setDownloadName(tag, "csv", true, userId)

    const dataURL = window.URL.createObjectURL(new Blob([csvUint8Array], {type: 'text/csv;charset=utf-8;'}))

    downloadFile(filename, dataURL)
}

/**
 * A function that takes an SVG held in memory and converts it to a csv file for downloading. This triggers the
 * downloading of the file.
 *
 * @remarks
 * The SVG passed as an argument is a node in the DOM, to create the image to download there is some processing
 * of the node required. This includes setting the right headers for the SVG, this is needed because when using
 * the 'react-svg-pan-zoom' plugin it removes the headers from the svg node and sets them on a wrapper element.
 *
 * @param svg - The SVG to download.
 * @param tag - The metadata tag for the data, used to set the download name.
 * @param userId - The user ID requesting the download, used to set the download name.
 * @param settings - The settings from how the user was viewing the SVG prior to
 * clicking to download the image. This is an object of parameters defined by the 'react-svg-pan-zoom'
 * plugin.
 */
export const downloadSvg = (svg: Node, tag: trac.metadata.ITag, userId: string, settings: Value) => {

    // Set the file name of the image
    const filename = setDownloadName(tag, "svg", true, userId)

    // Get SVG source and convert to a string.
    const serializer = new XMLSerializer();
    let source = serializer.serializeToString(svg);

    // Add name spaces to the svg element if present.
    if (!source.match(/^<svg[^>]+xmlns="http:\/\/www\.w3\.org\/2000\/svg"/)) {
        source = source.replace(/^<svg/, '<svg xmlns="http://www.w3.org/2000/svg"');
    }

    // Add the svg element if removed by ReactSVGPanZoom
    if (!source.match(/^<svg/)) {
        source = '<svg height="' + settings.SVGHeight + '" width="' + settings.SVGWidth + '" xmlns="http://www.w3.org/2000/svg">' + source + "</svg>"
    }

    // Remove the name space information from the g elements if added by ReactSVGPanZoom
    if (source.match(/<g xmlns="http:\/\/www\.w3\.org\/2000\/svg">/)) {
        source = source.replace(/<g xmlns="http:\/\/www\.w3\.org\/2000\/svg">/, '<g>');
    }

    // Add xml declaration
    source = '<?xml version="1.0" standalone="no"?>\r\n' + source;

    // Convert svg source to URI data scheme.
    const url = "data:image/svg+xml;charset=utf-8," + encodeURIComponent(source);

    downloadFile(filename, url)
}