/**
 * A component that shows the user to see a summary of a particular version of a run flow job stored in TRAC.
 * @module JobViewerRunFlow
 * @category ObjectSummaryScene Component
 */

import {buildParametersAcrossMultipleModels, setProcessedParameters} from "../../../utils/utils_flows";
import {Button} from "../../../components/Button";
import {Icon} from "../../../components/Icon";
import {isDefined} from "../../../utils/utils_trac_type_chckers";
import {Loading} from "../../../components/Loading";
import PropTypes from "prop-types";
import React, {useMemo} from "react";
import {ReviewDatasets} from "../../../components/ReviewDatasets";
import {ReviewFlow} from "../../../components/ReviewFlow";
import {ReviewModels} from "../../../components/ReviewModels";
import {ReviewParameters} from "../../../components/ReviewParameters";
import {setJobToRerun} from "../../RunAFlowScene/store/runAFlowStore";
import {tracdap as trac} from "@finos/tracdap-web-api";
import {useAppDispatch} from "../../../../types/types_hooks";
import {useNavigate} from "react-router-dom";
import {useMetadataStoreBatch} from "../../../utils/utils_async";

// A dummy object in order to prevent subcomponents re-rendering purely because a new object
// reference is created each render
const emptyObject = {}

/**
 * An interface for the props of the JobViewerRunFlow component.
 */
export interface Props {

    /**
     * The downloaded metadata for the selected job.
     */
    job: trac.metadata.ITag
}

export const JobViewerRunFlow = (props: Props) => {

    console.log("Rendering JobViewerRunFlow")

    const {job} = props

    // Set up the method used to update the redux store
    const dispatch = useAppDispatch()

    // A hook from the React Router plugin that allows us to navigate using onClick events, in this case we move to a
    // page that shows information about the selected object from the table
    const navigate = useNavigate()

    // Extract the information we need from the job definition, this provides the tag selectors for the items we need
    // as well as the keys that the job had for each of them. The trick here is that the tag selectors are returned 
    // with the same order we request them in, so we can use this to match their keys to the returned tags later
    const {tagSelectors, keys, count} = useMemo(() => {

        console.log("LOG :: Calculating tagSelectors, keys, count")

        // Create an array of all the tags needed to describe the job, only try and get the outputs if the job has succeeded
        const tagSelectors = [
            ...Object.values(job.definition?.job?.runFlow?.inputs || {}),
            ...Object.values(job.definition?.job?.runFlow?.models || {}),
            ...job.attrs?.trac_job_status.stringValue === "SUCCEEDED" ? Object.values(job.definition?.job?.runFlow?.outputs || {}) : []
        ]

        // Add a flow tag selector too, this component is only rendered if runFlow is set, but we should be compliant anyway
        if (job.definition?.job?.runFlow?.flow) {
            tagSelectors.push(job.definition?.job?.runFlow?.flow)
        }

        // Now create the keys that the flow used for each, output keys only exist if the job has succeeded
        const keys = {
            inputs: Object.keys(job.definition?.job?.runFlow?.inputs || {}),
            models: Object.keys(job.definition?.job?.runFlow?.models || {}),
            outputs: job.attrs?.trac_job_status.stringValue === "SUCCEEDED" ? Object.keys(job.definition?.job?.runFlow?.outputs || {}) : []
        }

        // Now create the counts for each type, this is required to extract the tags that are fetched into their constituent types
        // Note that we use the tags found as the count not the number of keys
        // TODO check that this logic holds when optional datasets are in the API
        const count = {
            inputs: Object.values(job.definition?.job?.runFlow?.inputs || {}).length,
            models: Object.values(job.definition?.job?.runFlow?.models || {}).length,
            outputs: (job.attrs?.trac_job_status.stringValue === "SUCCEEDED" ? Object.values(job.definition?.job?.runFlow?.outputs || {}) : []).length
        }

        return ({tagSelectors, keys, count})

    }, [job.attrs?.trac_job_status.stringValue, job.definition?.job?.runFlow?.flow, job.definition?.job?.runFlow?.inputs, job.definition?.job?.runFlow?.models, job.definition?.job?.runFlow?.outputs])

    // This is a custom hook which I am very proud of, it basically takes all the logic for getting an array of Tags  along with
    // the error handling and whether the tag is being downloaded into a hook just like useState, it also has logic to make sure that if
    // the tag has not changed the data is not fetched again. There can be a callback function that is passed which runs whenever the tag is
    // updated
    const [isDownloading, tags] = useMetadataStoreBatch(tagSelectors.filter(isDefined))

    // Combine the downloaded tags into objects that can be passed down to the subcomponents to display
    const {flow, inputs, models, outputs} = useMemo(() => {

        console.log("LOG :: Calculating flow, inputs, models, outputs")

        // Convert the downloaded tags into objects that can be passed to the subcomponents
        // Flows are optionally added to the tags, so we need to check here if we did indeed request a flow
        const flow = tags[tags.length - 1]?.header?.objectType === trac.ObjectType.FLOW ? tags[tags.length - 1] : undefined
        const inputs: Record<string, trac.metadata.ITag> = {}
        const models: Record<string, trac.metadata.ITag> = {}
        const outputs: Record<string, trac.metadata.ITag> = {}

        tags.filter((tag, i) => i >= 0 && i < count.inputs).forEach((tag, i) => inputs[keys.inputs[i]] = tag)
        tags.filter((tag, i) => i >= count.inputs && i < count.inputs + count.models).forEach((tag, i) => models[keys.models[i]] = tag)
        tags.filter((tag, i) => i >= count.inputs + count.models && i < count.inputs + count.models + count.outputs).forEach((tag, i) => outputs[keys.outputs[i]] = tag)

        return ({flow, inputs, models, outputs})

    }, [count.inputs, count.models, count.outputs, keys.inputs, keys.models, keys.outputs, tags])

    // The job definition does not contain the information about the parameter labels, that is in the model metadata
    // used to create the job. In the runAFlowStore these are functions that process all the model metadata into
    // a single object. Here we use the same functions so that we can create the same parameter definitions and use
    // this to augment the view of the flow.
    let parametersFromModels = buildParametersAcrossMultipleModels(models)

    // Convert the model parameters into parameter definitions that can be used by the ParameterMenu component.
    // So if the parameter has a label set in the model then these are used.
    let parameterDefinitions = setProcessedParameters(parametersFromModels)

    return (

        <React.Fragment>

            {isDownloading &&
                <Loading text={"Please wait..."}/>
            }

            {!isDownloading && flow &&

                <React.Fragment>

                    <ReviewFlow storeKey={"jobViewerRunFlow"}
                                tag={flow}
                    />

                    <ReviewParameters definitions={parameterDefinitions}
                                      storeKey={"jobViewerRunFlow"}
                                      values={job.definition?.job?.runFlow?.parameters ?? emptyObject}
                    />

                    <ReviewModels models={models}
                                  showOnOpen={false}
                                  storeKey={"jobViewerRunFlow"}
                    />

                    <ReviewDatasets datasets={inputs}
                                    showOnOpen={true}
                                    storeKey={"jobViewerRunFlow"}
                                    type={"inputs"}
                    />

                    {/*There will not be any outputs if the job failed*/}
                    {outputs && Object.keys(outputs).length > 0 &&
                        <ReviewDatasets datasets={outputs}
                                        showOnOpen={true}
                                        storeKey={"jobViewerRunFlow"}
                                        type={"outputs"}
                        />
                    }

                    <Button ariaLabel={"Rerun job"}
                            className={"float-end min-width-px-150 mt-4"}
                            isDispatched={false}
                            onClick={() => {
                                dispatch(setJobToRerun({job, flow}));
                                navigate("../../run-a-flow")
                            }}
                    >
                        <Icon ariaLabel={false} className={"me-2"} icon={"bi-arrow-repeat"}/>Re-run job
                    </Button>

                </React.Fragment>
            }

        </React.Fragment>
    )
};

JobViewerRunFlow.propTypes = {

    job: PropTypes.object.isRequired
};