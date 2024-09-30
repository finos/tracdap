/**
 * A component that shows a table of files attached to an object such as a model or a dataset.
 * // TODO add in pdf version
 // * This component is for
 // * viewing in a browser, there is a sister component called {@link ParameterTablePdf} that is for viewing in a
 // * PDF. These two components need to be kept in sync so if a change is made to one then it should be reflected in
 // * the other.
 * @module AttachedFileTable
 * @category Component
 */

import {createUniqueObjectKey} from "../utils/utils_trac_metadata";
import {Icon} from "./Icon";
import {isObject} from "../utils/utils_trac_type_chckers";
import PropTypes from "prop-types";
import React, {useEffect, useState} from "react";
import Table from "react-bootstrap/Table";
import {tracdap as trac} from "@finos/tracdap-web-api";
import {useAppDispatch, useAppSelector} from "../../types/types_hooks";
import {searchForAttachedFile} from "../utils/utils_trac_api";
import {setLanguageOrFileIcon, showToast} from "../utils/utils_general";

/**
 * An interface for the props of the AttachedFileTable component.
 */
export interface Props {

    /**
     * The css class to apply to the table, this allows additional styles to be added to the component.
     */
    className?: string
    /**
     * The TRAC tag selector for the object (data or model etc.) to search for attached files.
     */
    tagSelector?: null | trac.metadata.ITagSelector
}

export const AttachedFileTable = (props: Props) => {

    const {className = "", tagSelector} = props

    // Get what we need from the store
    const {"trac-tenant": tenant} = useAppSelector(state => state["applicationStore"].cookies)
    const {searchAsOf} = useAppSelector(state => state["applicationStore"].tracApi)

    const [{attachedFiles}, setState] = useState<{
        isDownloading: boolean,
        attachedFiles: trac.metadata.ITag[]
    }>({
        isDownloading: false,
        attachedFiles: []
    })

    // Set up the method used to update the redux store
    const dispatch = useAppDispatch()

    /**
     * A hook that makes an API call to get the metadata and object information if they are not supplied
     * as props.
     */
    useEffect(() => {

        // We can't call async functions with await directly in useEffect so we have to define our
        // async function wrapper inside.
        // See https://stackoverflow.com/questions/53332321/react-hook-warnings-for-async-function-in-useeffect-useeffect-function-must-ret
        // TODO in React 18 this needs to be updated to Suspense to avoid race conditions
        async function fetchMyAPI(tagHeader: trac.metadata.ITagHeader, tenant: string): Promise<trac.metadata.ITag[]> {

            try {
                // Update state
                setState(prevState => ({...prevState, isDownloading: true}))

                return await searchForAttachedFile({attachedObjectKey: createUniqueObjectKey(tagHeader, false), searchAsOf, tenant})

            } catch (err) {

                setState(prevState => ({...prevState, isDownloading: false}))

                const text = {
                    title: "Failed to search for attached files",
                    message: "The request to search for the object's attached files did not complete successfully.",
                    details: typeof err === "string" ? err : isObject(err) && typeof err.message === "string" ? err.message : undefined
                }

                showToast("error", text, "FileTable/fetchMyAPI/rejected")

                if (typeof err === "string") {
                    throw new Error(err)
                } else {
                    console.error(err)
                    throw new Error("Search failed")
                }
            }
        }

        //  If the object tagHeader is not set then we can not get the metadata
        if (tagSelector != null && tenant !== undefined) fetchMyAPI(tagSelector, tenant).then(tags => {

            // Update state
            setState(prevState => ({
                ...prevState,
                attachedFiles: tags,
                isDownloading: false
            }))
        })

    }, [dispatch, tagSelector, tenant])

    return (

        <React.Fragment>

            {(!attachedFiles || attachedFiles.length === 0) &&
                <div className={"pb-1"}>There are no attached documents</div>
            }

            {attachedFiles && attachedFiles.length > 0 &&
                <Table striped responsive className={className} size={"sm"}>
                    <thead>
                    <tr>
                        <th>Name</th>
                        <th>Description</th>
                        <th className={"text-center"}>Download</th>
                    </tr>
                    </thead>
                    <tbody>
                    {attachedFiles.map((file, i) => {

                        return (
                            <tr key={i}>
                                <td className={"align-middle"}>{file?.attrs?.name.stringValue || file?.attrs?.trac_file_name.stringValue || "Not set"}</td>
                                <td className={"align-middle"}>{file?.attrs?.description.stringValue || "Not set"}</td>
                                <td className={" align-middle text-center"}>
                                    <Icon ariaLabel={"File"}
                                          className={"pointer"}
                                        //TODO they are not all pdfs
                                          icon={setLanguageOrFileIcon("pdf")}
                                          size={"1.5rem"}
                                    />
                                </td>

                            </tr>
                        )
                    })}
                    </tbody>
                </Table>
            }

        </React.Fragment>
    )
};

AttachedFileTable.propTypes = {

    className: PropTypes.string,
    tagSelector: PropTypes.object
};