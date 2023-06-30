/**
 * A component that shows the user to see a summary of a particular version of an import model job stored in TRAC.
 * This component is for viewing in a browser, there is a sister component called {@link JobViewerImportModelPdf}
 * that is for viewing in a PDF. These two components need to be kept in sync so if a change is made to one then
 * it should be reflected in the other.
 * @module JobViewerImportModel
 * @category ObjectSummaryScene Component
 */

import {Button} from "../../../components/Button";
import {getModelFromImportJob, importModel} from "../../../utils/utils_trac_api";
import {HeaderTitle} from "../../../components/HeaderTitle";
import {Icon} from "../../../components/Icon";
import {isObject} from "../../../utils/utils_trac_type_chckers";
import {showToast} from "../../../utils/utils_general";
import {ObjectDetails} from "../../../components/ObjectDetails";
import {ObjectSummaryPaths} from "../../../../config/config_menu";
import PropTypes from "prop-types";
import React, {useEffect, useState} from "react";
import {ShowHideDetails} from "../../../components/ShowHideDetails";
import {TextBlock} from "../../../components/TextBlock";
import {tracdap as trac} from "@finos/tracdap-web-api";
import {useAppSelector} from "../../../../types/types_hooks";
import {useNavigate} from "react-router-dom";

// This is a list of the metadata attributes from the job that we want to show that relate to the
// model that was imported. A tag that is an array means that the property is deeply nested.
const modelPropertiesToShow: { tag: "attrs" | "header" | string[], property: string }[] = [
    {tag: ["definition", "job", "importModel"], property: "entryPoint"},
    {tag: ["definition", "job", "importModel"], property: "language"},
    {tag: ["definition", "job", "importModel"], property: "path"},
    {tag: ["definition", "job", "importModel"], property: "repository"},
    {tag: ["definition", "job", "importModel"], property: "version"}
]

// This is a list of the metadata attributes that were added to the model when the job was run. modelAttrs is an array
// of tag updates.
const modelAttrsToShow: { tag: "attrs" | "header" | string[], property: string }[] = [
    {tag: ["definition", "job", "importModel"], property: "modelAttrs"}
]

/**
 * A function that runs when the user clicks to re-import a model that has failed to load, it uses the failed
 * job's metadata to set the payload to import the model and then makes the API call.
 */
const handleReUploadModel = async (tenant: string, job: trac.metadata.ITag): Promise<void> => {

    // Recreate the job import specification
    const importDetails: trac.metadata.IImportModelJob = {

        language: job?.definition?.job?.importModel?.language,
        repository: job?.definition?.job?.importModel?.repository,
        path: job?.definition?.job?.importModel?.path,
        entryPoint: job?.definition?.job?.importModel?.entryPoint,
        version: job?.definition?.job?.importModel?.version,
        // This copies all the attrs across to the new model
        modelAttrs: job?.definition?.job?.importModel?.modelAttrs
    }

    try {

        const jobStatus = await importModel({importDetails, tenant})

        // TODO update the record to say this model was re-uploaded. Or make this only for failed loads
        showToast("success", `The job to load the model into TRAC was successfully restarted with job ID ${jobStatus.jobId?.objectId}, you can see its progress in the 'Find a job' and 'My jobs' pages.`, "handleReUploadModel/fulfilled")

    } catch (error) {

        const text = {
            title: "Failed to load the model",
            message: "The job to import the model was not successfully submitted.",
            details: typeof error === "string" ? error : isObject(error) && typeof error.message === "string" ? error.message : undefined
        }

        showToast("error", text, "handleReUploadModel/rejected")

        if (typeof error === "string") {
            throw new Error(error)
        } else {
            console.error(error)
            throw new Error("Model re-import job submission failed")
        }
    }
}

/**
 * An interface for the props of the JobViewerImportModel component.
 */
export interface Props {

    /**
     * The downloaded metadata for the selected job.
     */
    job: trac.metadata.ITag
}

export const JobViewerImportModel = (props: Props) => {

    const {job} = props

    const [model, setState] = useState<undefined | trac.metadata.ITag>(undefined)

    // Get what we need from the store
    const {"trac-tenant": tenant} = useAppSelector(state => state["applicationStore"].cookies)
    const {searchAsOf} = useAppSelector(state => state["applicationStore"].tracApi)

    // A hook from the React Router plugin that allows us to navigate using onClick events, in this case we move to a
    // page that shows information about the selected object from the table
    const navigate = useNavigate()

    /**
     * A function that runs when the user clicks to view the model that the job imported, it checks that the right
     * information is available and if so it navigates to the right page using the URL parameters to say what
     * object to view.
     */
    const handleViewModel = () => {

        if (model?.header?.objectType == null || model?.header?.objectId == null || model?.header?.objectVersion == null || model?.header?.tagVersion == null) {
            throw new Error("The model tag does not have all of the information required to view the object")
        }

        navigate(`${ObjectSummaryPaths[model?.header?.objectType].to}/${model?.header?.objectId}/${model?.header?.objectVersion}/${model?.header?.tagVersion}`)
    }

    /**
     * A hook that makes an API call to get the tag of the model that was added as part of the import model job.
     */
    useEffect(() => {

        // We can't call async functions with await directly in useEffect so we have to define our
        // async function wrapper inside.
        // See https://stackoverflow.com/questions/53332321/react-hook-warnings-for-async-function-in-useeffect-useeffect-function-must-ret
        async function fetchMyAPI(tenant: string, job: trac.metadata.ITag): Promise<undefined | trac.metadata.ITag[]> {

            try {

                return await getModelFromImportJob({
                    tenant,
                    searchAsOf,
                    objectKey: `JOB-${job.header?.objectId}-v1`
                })

            } catch (error) {

                const text = {
                    title: "Failed to download the metadata",
                    message: "The request to download the metadata for the model that the job created did not complete successfully.",
                    details: typeof error === "string" ? error : isObject(error) && typeof error.message === "string" ? error.message : undefined
                }

                showToast("error", text, "metadata-error")

                if (typeof error === "string") {
                    throw new Error(error)
                } else {
                    console.error(error)
                    throw new Error("Metadata download failed")
                }
            }
        }

        if (tenant !== undefined && job.header?.objectId && job?.attrs?.trac_job_status?.stringValue === "SUCCEEDED") {
            fetchMyAPI(tenant, job).then(searchResults => setState(searchResults && searchResults.length > 0 ? searchResults[0] : undefined))
        }

        // This is a state cleanup function that runs when the component unmounts
        return () => {
            setState(undefined);
        };

    }, [job, job?.attrs?.trac_job_status?.stringValue, job.header?.objectId, searchAsOf, tenant])

    return (

        <React.Fragment>

            <HeaderTitle type={"h3"} text={"Model location"}/>

            <TextBlock>
                Information about the location of the model that was imported. The repository is configured as part of the TRAC services. The path and entry point detail the path to the
                script, while the version corresponds to the commit that the model was loaded from.
            </TextBlock>

            <ShowHideDetails linkText={"model details"} classNameOuter={"mt-0"} showOnOpen={true}>
                <ObjectDetails bordered={false}
                               metadata={job}
                               propertiesToShow={modelPropertiesToShow}
                               striped={true}
                />
            </ShowHideDetails>


            <HeaderTitle type={"h3"} text={"Model metadata"}>
                {model &&
                    <Button ariaLabel={"View model"}
                            className={"min-width-px-150"}
                            isDispatched={false}
                            onClick={handleViewModel}
                            variant={"outline-info"}
                    >
                        <Icon ariaLabel={false} className={"me-2"} icon={"bi-binoculars"}/>View model
                    </Button>
                }
            </HeaderTitle>

            <TextBlock>
                Information about the metadata that was attached to the loaded model.
            </TextBlock>

            <ShowHideDetails classNameOuter={"mt-0"} linkText={"model metadata"} showOnOpen={true}>
                <ObjectDetails bordered={false}
                               metadata={job}
                               propertiesToShow={modelAttrsToShow}
                               striped={true}
                />
            </ShowHideDetails>

            {job?.attrs?.trac_job_status?.stringValue === "FAILED" && tenant !== undefined &&
                <Button ariaLabel={"Re-import model"}
                        className={"ms-2 float-end min-width-px-150"}
                        isDispatched={false}
                        onClick={() => handleReUploadModel(tenant, job)}
                >
                    <Icon ariaLabel={false} className={"me-2"} icon={"bi-arrow-repeat"}/>Re-import model
                </Button>
            }

        </React.Fragment>
    )
};

JobViewerImportModel.prototype = {

    job: PropTypes.object.isRequired
};