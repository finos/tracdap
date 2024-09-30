import Col from "react-bootstrap/Col";
import {NodeDetails, Option, SelectOptionPayload} from "../../../../types/types_general";
import {ObjectSummaryStoreState, setSelectedNodeId} from "../store/objectSummaryStore";
import PropTypes from "prop-types";
import React, {useCallback} from "react";
import Row from "react-bootstrap/Row";
import {SelectOption} from "../../../components/SelectOption";
import {tracdap as trac} from "@finos/tracdap-web-api";
import {useAppDispatch, useAppSelector} from "../../../../types/types_hooks";

// This is for a blank column so that the layout with a vertical toolbar is symmetric. The min
// width needs to be set to the width of the toolbar
const blankColStyle = {minWidth: "50px"}

/**
 * A component that shows a SelectOption component that allows the user to select a node in the SVG to
 * see the information for.
 */

type Props = {

    /**
     * The key in the objectSummaryStore to get the state for this component.
     */
    storeKey: keyof ObjectSummaryStoreState["flow"]
};

const SelectFlowNode = (props: Props) => {

    console.log("Rendering SelectFlowNode")

    const {storeKey} = props

    // Get what we need from the store
    const {
        optionsWithoutRenames,
        optionsWithRenames,
        selectedNodeOption
    } = useAppSelector(state => state["objectSummaryStore"].flow[storeKey].nodes)

    const {showRenames} = useAppSelector(state => state["objectSummaryStore"].flow[storeKey].chart)

    // Set up the method used to update the redux store
    const dispatch = useAppDispatch()

    // The setSelectedNodeId function in the ViewAFlowStore does not use the usual payload
    // the payload is adjusted to be the id of the item selected and the function then
    // sets the option as well as a bunch of other variables
    const handleOptionChange = useCallback((payload : SelectOptionPayload<Option<string, NodeDetails>, false>) => {

        dispatch(setSelectedNodeId({storeKey, selectedNodeId: payload.value?.value ?? null}))

    }, [dispatch, storeKey])

    return (
        <Row className={"my-3 pb-3"}>

            {/*This column is so that the components below line up with the SVG image as that has margins */}
            {/*either side to allow for the toolbar*/}
            <Col style={blankColStyle} xs={"auto"} className={"d-none d-md-block p-0"}>&nbsp;</Col>

            <Col className={"my-auto p-md-0"} xs={12} md={8} lg={6}>

                <SelectOption basicType={trac.STRING}
                              isDispatched={false}
                              labelText={"Select a node:"}
                              mustValidate={false}
                              onChange={handleOptionChange}
                              options={showRenames ? optionsWithRenames : optionsWithoutRenames}
                              showValidationMessage={false}
                              validateOnMount={false}
                              value={selectedNodeOption}
                />

            </Col>

            {/*This column is so that the components below line up with the SVG image as that has margins */}
            {/*either side to allow for the toolbar*/}
            <Col style={blankColStyle} xs={"auto"} className={"d-none d-md-block p-0"}>&nbsp;</Col>
        </Row>
    )
};

SelectFlowNode.propTypes = {

    storeKey: PropTypes.string.isRequired,
};

export default SelectFlowNode;