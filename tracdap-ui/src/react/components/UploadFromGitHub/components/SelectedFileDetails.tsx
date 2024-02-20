/**
 * A component that shows information about the selected file to upload to TRAC. This can handle both model and schema
 * loading from GitHub. If the config for the application is set to not allow duplicate objects to be loaded then
 * this component will message if the loading of their selected file is restricted.
 *
 * When the object being loaded is a model additional messages about whether any issues
 * were identified such as no entry point being found are shown, it allows the user to select the value of
 * certain parameters needed to load the model. If the model has already been found in TRAC then a
 * message is also shown with a link to the model.
 *
 * When the object being loaded is a schema then messages are shown if the file was found to not meet the requirements
 * of a TRAC schema.
 *
 * @module SelectedFileDetails
 * @category Component
 */

import {A} from "../../A";
import {Alert} from "../../Alert";
import Col from "react-bootstrap/Col";
import {General} from "../../../../config/config_general";
import {humanReadableFileSize} from "../../../utils/utils_number";
import {Icon} from "../../Icon";
import {Link} from "react-router-dom";
import {Loading} from "../../Loading";
import {ObjectSummaryPaths} from "../../../../config/config_menu";
import PropTypes from "prop-types";
import React from "react";
import Row from "react-bootstrap/Row"
import {SelectOption} from "../../SelectOption";
import {setLanguageOrFileIcon} from "../../../utils/utils_general";
import {setTracModelClass, type UploadFromGitHubStoreState} from "../store/uploadFromGitHubStore";
import {tracdap as trac} from "@finos/tracdap-web-api";
import {useAppSelector} from "../../../../types/types_hooks";
import {SchemaFieldsTable} from "../../SchemaFieldsTable/SchemaFieldsTable";

// Some Bootstrap grid layouts
const lgGrid = {span: 8, offset: 2};
const mdGrid = {span: 10, offset: 1};
const xsGrid = {span: 12, offset: 0};

/**
 * An interface for the props of the SelectedFileDetails component.
 */
export interface Props {

    /**
     * The key in the UploadFromGitHubStore to get the state for this component
     */
    storeKey: keyof UploadFromGitHubStoreState["uses"]
    /**
     * The type of file being selected, it is used in messages to the user.
     */
    uploadType: "model" | "schema"
}

export const SelectedFileDetails = (props: Props) => {

    const {storeKey, uploadType} = props

    // Get what we need from the store
    const {
        branch: {commit},
        file: {
            details,
            status,
            metadata,
            alreadyInTrac,
            path,
            model: {
                entryPoint,
                tracModelClassOptions,
                selectedTracModelClassOption,
            },
            schema: {
                fields,
                errorMessages
            }
        },
        tree
    } = useAppSelector(state => state["uploadFromGitHubStore"].uses[storeKey])

    return (
        <React.Fragment>
            {status === "pending" &&
                <Loading text={false}/>
            }

            {/*Show once a call has been completed to get file information as well as the relevant tree and commit options selected */}
            {/*tracModelClassOptions is an array of the classes in the code file that use the TRAC model API, at least one*/}
            {/*is needed for a model to load successfully, these are ponly shown if the object being loaded is a model.*/}
            {status === "succeeded" && commit.selectedOption && details && tree.selectedOption &&

                <React.Fragment>

                    <Row className={"mt-5 mt-lg-3"}>
                        <Col xs={xsGrid} md={mdGrid} lg={lgGrid} className={"fs-9 d-flex align-items-center"}>
                            <Icon ariaLabel={"Selected file"}
                                  className={"me-3"}
                                  icon={setLanguageOrFileIcon(tree.selectedOption.fileExtension)}
                            />
                            <span className={"fs-7 fw-bold"}>Selected file:</span>
                        </Col>
                    </Row>

                    <Row>
                        <Col xs={xsGrid} md={mdGrid} lg={lgGrid}>
                            <table
                                className={`dataHtmlTable w-100 mt-3 ${tracModelClassOptions.length !== 0 ? "mb-3" : ""}`}>
                                <tbody>
                                <tr>
                                    <td className={"text-nowrap w-25"}>Path:</td>
                                    <td className={"text-nowrap w-75"}>{path}</td>
                                </tr>
                                <tr>
                                    <td className={"text-nowrap"}>Size:</td>
                                    <td className={"text-nowrap"}>{tree.selectedOption.size ? humanReadableFileSize(tree.selectedOption.size) : "Unknown"}</td>
                                </tr>
                                <tr>
                                    <td className={"text-nowrap"}>Commit SHA:</td>
                                    <td className={"text-nowrap"}>{commit.selectedOption.details.commit.sha}</td>
                                </tr>
                                <tr>
                                    <td className={"text-nowrap"}>File SHA:</td>
                                    <td className={"text-nowrap"}>{tree.selectedOption.sha}</td>
                                </tr>
                                <tr>
                                    <td className={"text-nowrap"}>Links:</td>
                                    <td className={"text-nowrap"}>
                                        {/* The request to get files from a GitHub tree can return an array of objects for a folder, we only get files*/}
                                        {/* so the response will be an object, however we use the GitHub API type interfaces, so we have ti type guard here.*/}
                                        {!Array.isArray(details) && "html_url" in details && details.html_url ? <A href={details.html_url}>File</A> : ""}{" | "}<A
                                        href={commit.selectedOption.details.commit.html_url}>Commit</A>{metadata.hasMetadata && metadata?.url && " | "}{metadata.hasMetadata && metadata?.url &&
                                        <A href={metadata?.url || "unknown"}>Metadata</A>}
                                    </td>
                                </tr>
                                <tr>
                                    <td className={"text-nowrap"}>Metadata found in repo<Icon ariaLabel={"More information"}
                                                                                              className={"ms-2"}
                                                                                              icon={"bi-question-circle"}
                                                                                              tooltip={metadata.filename == null ? "No information about the metadata files in this repository were defined in the application config, so no search for the metadata was made." : `The application checked for a file called ${metadata.filename} in the same folder as the selected file to use to provide metadata for the ${uploadType} upload`}/> :
                                    </td>
                                    <td className={"text-nowrap"}>
                                        {metadata.hasMetadata ? "True" : "False"}{metadata.hasMetadata && !metadata.hasValidMetadata && " (metadata was invalid)"}
                                    </td>
                                </tr>
                                {storeKey === "uploadAModel" &&
                                    <React.Fragment>
                                        <tr>
                                            <td className={"text-nowrap"}>Class<Icon ariaLabel={"More information"}
                                                                                     className={"ms-2"}
                                                                                     icon={"bi-question-circle"}
                                                                                     tooltip={`The class defined in the ${uploadType} code that is called to execute the ${uploadType}`}
                                            /> :
                                            </td>
                                            <td className={"text-nowrap"}>
                                                {tracModelClassOptions.length === 0 ? "Not found" : tracModelClassOptions.length === 1 ? tracModelClassOptions[0].value :
                                                    <SelectOption basicType={trac.STRING}
                                                                  className={"py-1"}
                                                                  isDisabled={Boolean(status !== "succeeded")}
                                                                  name={storeKey}
                                                                  onChange={setTracModelClass}
                                                                  options={tracModelClassOptions}
                                                                  placeHolderText={Boolean(status !== "succeeded") ? "Loading..." : undefined}
                                                                  value={selectedTracModelClassOption}
                                                                  validateOnMount={false}
                                                    />
                                                }
                                            </td>
                                        </tr>
                                        <tr>
                                            <td className={"text-nowrap"}>
                                                Entry point <Icon ariaLabel={"More information"}
                                                                  icon={"bi-question-circle"}
                                                                  tooltip={`The path from the root of the python packages to the model class`}/> :
                                            </td>
                                            <td className={"text-nowrap"}>
                                                {entryPoint ? entryPoint : "Unavailable"}
                                            </td>
                                        </tr>
                                    </React.Fragment>
                                }
                                </tbody>
                            </table>

                        </Col>
                        <Col xs={xsGrid} md={mdGrid} lg={lgGrid}>

                            {storeKey === "uploadAModel" && tracModelClassOptions.length === 0 &&
                                <Alert className={"my-3"} variant={"danger"}>
                                    This model can not be loaded as the entrypoint class could not be identified.
                                    This is the class that is called to run the model code and is usually passed the
                                    TRAC model API.
                                </Alert>
                            }

                            {storeKey === "uploadASchema" && errorMessages.length > 0 &&
                                <Alert className={"my-3"} variant={"danger"}>
                                    {["This schema can not be loaded as it failed validation. The following issues were found:"].concat(errorMessages)}
                                </Alert>
                            }

                            {storeKey === "uploadAModel" && !General.loading.allowCopies.model && alreadyInTrac.foundInTrac && alreadyInTrac.tag?.header?.objectType &&
                                <Alert className={"my-3"} variant={"warning"}>
                                    <div>
                                        The version of the model stored in this commit has already been loaded into
                                        TRAC and can not be loaded again. The object ID of the model
                                        is <Link className={"alert-link"}
                                                 to={`${ObjectSummaryPaths[alreadyInTrac.tag.header.objectType].to}/${alreadyInTrac.tag.header.objectId}/${alreadyInTrac.tag.header.objectVersion}/${alreadyInTrac.tag.header.tagVersion}/`}
                                    >{alreadyInTrac.tag.header?.objectId}</Link> and was last updated by the job with object key <span
                                        className={"user-select-all"}>{alreadyInTrac.tag.attrs?.trac_update_job.stringValue}</span>
                                    </div>
                                </Alert>
                            }

                            {storeKey === "uploadASchema" && !General.loading.allowCopies.schema && alreadyInTrac.foundInTrac && alreadyInTrac.tag?.header?.objectType &&
                                <Alert className={"my-3"} variant={"warning"}>
                                    <div>
                                        The version of the schema stored in this commit has already been loaded into
                                        TRAC and can not be loaded again. The object ID of the schema
                                        is <Link className={"alert-link"}
                                                 to={`${ObjectSummaryPaths[alreadyInTrac.tag.header.objectType].to}/${alreadyInTrac.tag.header.objectId}/${alreadyInTrac.tag.header.objectVersion}/${alreadyInTrac.tag.header.tagVersion}/`}
                                    >{alreadyInTrac.tag.header?.objectId}</Link>
                                    </div>
                                </Alert>
                            }

                        </Col>

                        {storeKey === "uploadASchema" && errorMessages.length === 0 &&
                            <Col xs={xsGrid} md={mdGrid} lg={lgGrid}>

                                <SchemaFieldsTable className={"mt-5"} fields={fields}/>

                            </Col>
                        }
                    </Row>
                </React.Fragment>
            }
        </React.Fragment>
    )
};

SelectedFileDetails.propTypes = {

    storeKey: PropTypes.string.isRequired,
    uploadType: PropTypes.oneOf(["model", "schema"]).isRequired
};