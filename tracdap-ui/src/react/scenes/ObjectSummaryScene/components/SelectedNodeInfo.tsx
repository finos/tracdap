import {capitaliseString} from "../../../utils/utils_string";
import Col from "react-bootstrap/Col";
import {convertFlowNodeTypeToString} from "../../../utils/utils_trac_metadata";
import {HeaderTitle} from "../../../components/HeaderTitle";
import {ModelDatasetListTable} from "../../../components/ModelDatasetListTable";
import {ObjectSummaryStoreState} from "../store/objectSummaryStore";
import PropTypes from "prop-types";
import React from "react";
import Row from "react-bootstrap/Row";
import SearchQueryListTable from "./SearchQueryListTable";
import {TextBlock} from "../../../components/TextBlock";
import {tracdap as trac} from "@finos/tracdap-web-api";
import {useAppSelector} from "../../../../types/types_hooks";

// This is for a blank column so that the layout with a vertical toolbar is symmetric. The min
// width needs to be set to the width of the toolbar
const blankColStyle = {minWidth: "50px"}

/**
 * A component that shows information about the user's selected model in a flow.
 */

type Props = {

    /**
     * The key in the objectSummaryStore to get the state for this component
     */
    storeKey: keyof ObjectSummaryStoreState["flow"]
};

const SelectedNodeInfo = (props: Props) => {

    const {storeKey} = props

    // Get what we need from the store
    const {
        selectedDataset,
        selectedModelInputs,
        selectedNodeOption,
        selectedModelOutputs,
        selectedNodeSearchQuery
    } = useAppSelector(state => state["objectSummaryStore"].flow[storeKey].nodes)

    const flowNodeAsString = selectedNodeOption ? convertFlowNodeTypeToString(selectedNodeOption.details.type) : ""

    return (

        <React.Fragment>

            {selectedNodeOption &&

                <Row>
                    {/*This column is so that the components below line up with the SVG image as that has margins */}
                    {/*either side to allow for the toolbar*/}
                    <Col style={blankColStyle} xs={"auto"} className={"d-none d-md-block p-0"}>&nbsp;</Col>

                    <Col className={"my-auto pe-md-2 ps-md-2 py-md-0"}>

                        <HeaderTitle text={`${capitaliseString(flowNodeAsString)}: ${selectedNodeOption.label}`}
                                     type={"h4"}/>

                        {selectedNodeOption.details.type === trac.FlowNodeType.INPUT_NODE &&
                            <ModelDatasetListTable datasets={selectedDataset} datasetType={"input"}/>
                        }

                        {selectedNodeOption.details.type === trac.FlowNodeType.OUTPUT_NODE &&
                            <ModelDatasetListTable datasets={selectedDataset} datasetType={"output"}/>
                        }

                        {selectedNodeOption.details.type !== trac.FlowNodeType.OUTPUT_NODE && selectedNodeOption.details.type !== trac.FlowNodeType.NODE_TYPE_NOT_SET &&
                            <React.Fragment>
                                <HeaderTitle type={"h5"} text={"Search query"}/>

                                <TextBlock className={"mb-3"}>
                                    The search query is how TRAC finds the set of available options for
                                    this {flowNodeAsString} in the flow.
                                </TextBlock>

                                <SearchQueryListTable queries={selectedNodeSearchQuery}/>
                            </React.Fragment>
                        }

                        {selectedNodeOption.details.type === trac.FlowNodeType.MODEL_NODE &&
                            <React.Fragment>

                                <HeaderTitle type={"h5"} text={"Input datasets"}/>
                                <ModelDatasetListTable datasets={selectedModelInputs} datasetType={"input"}/>

                                <HeaderTitle type={"h5"} text={"Output datasets"}/>
                                <ModelDatasetListTable datasets={selectedModelOutputs} datasetType={"output"}/>

                            </React.Fragment>
                        }

                        {selectedNodeOption.details.type === trac.FlowNodeType.NODE_TYPE_NOT_SET &&
                            <TextBlock className={"mb-3"}>
                                This is an intermediate dataset, it is calculated as part of the flow but is not saved
                                with the other outputs.
                            </TextBlock>
                        }

                    </Col>

                    {/*This column is so that the components below line up with the SVG image as that has margins */}
                    {/*either side to allow for the toolbar*/}
                    <Col style={blankColStyle} xs={"auto"} className={"d-none d-md-block p-0"}>&nbsp;</Col>
                </Row>
            }

        </React.Fragment>
    )
};

SelectedNodeInfo.propTypes = {

    storeKey: PropTypes.string.isRequired
};

export default SelectedNodeInfo;