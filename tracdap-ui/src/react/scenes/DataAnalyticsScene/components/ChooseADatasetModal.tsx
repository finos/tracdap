import {Button} from "../../../components/Button";
import Col from "react-bootstrap/Col";
import Modal from "react-bootstrap/Modal"
import PropTypes from "prop-types";
import React from "react";
import Row from "react-bootstrap/Row";
import {tracdap as trac} from "@finos/tracdap-web-api";
import {FindInTrac} from "../../../components/FindInTrac";
import {setSelectedTags} from "../store/dataAnalyticsStore";
import {useAppDispatch, useAppSelector} from "../../../../types/types_hooks";

/**
 * A component that shows a modal with a menu that allows a user to select a dataset to build a SQL for..
 */

type Props = {

    /**
     * Whether to show the modal or not.
     */
    show: boolean
    // /**
    //  * The object's metadata tag.
    //  */
    // tag: trac.metadata.ITag
    // /**
    //  * The title for the modal.
    //  */
    // title: string
    /**
     * A function that toggles whether the modal is shown or not.
     */
    toggle: () => void
};

const ChooseADatasetModal = (props: Props) => {

    const {show, toggle} = props

    // Get what we need from the store, we pass this to the dataAnalyticsStore when the user saves their choices.
    const {selectedTab} = useAppSelector(state => state["findInTracStore"].uses.dataAnalytics)
    const tags = useAppSelector(state => state["findInTracStore"].uses.dataAnalytics.selectedTags[selectedTab])

    // Set up the method used to update the redux store
    const dispatch = useAppDispatch()

    return (

        <Modal size={"xl"} show={show} onHide={toggle}>

            <Modal.Header closeButton>
                <Modal.Title>
                    Select a dataset to query
                </Modal.Title>
            </Modal.Header>

            <Modal.Body>

                <div className={"mb-3 mx-3"}>
                    <FindInTrac storeKey={"dataAnalytics"}/>
                </div>

            </Modal.Body>
            <Modal.Footer>

                <Button ariaLabel={"Close"}
                        isDispatched={false}
                        onClick={toggle}
                        variant={"secondary"}>
                    Close
                </Button>

                <Button ariaLabel={"Save"}
                        isDispatched={false}
                        onClick={() => {dispatch(setSelectedTags(tags)); toggle()}}
                        variant={"info"}>
                    Save
                </Button>

            </Modal.Footer>
        </Modal>
    )
};

ChooseADatasetModal.propTypes = {

    show: PropTypes.bool.isRequired,
    toggle: PropTypes.func.isRequired
};

export default ChooseADatasetModal;