import {Button} from "../../../components/Button";
import SelectDatasetModal from "./ChooseADatasetModal";
import Col from "react-bootstrap/Col";
import React, {useCallback, useState} from "react";
import Row from "react-bootstrap/Row";

/**
 * A component that controls the modal that allows the user to select a dataset to query.
 */

const OpenModalToSelectDataset = () => {

    console.log("Rendering OpenModalToSelectDataset")

    const [showChooseDataModal, setShowChooseDataModal] = useState(false)

    // // Get what we need from the store
    // const {status} = useAppSelector(state => state["uploadADatasetStore"].import)

    /**
     * A function that runs when the user opens or closes the modal.
     */
    const onToggleImportModal = useCallback((): void => {

        setShowChooseDataModal(!showChooseDataModal)

    }, [showChooseDataModal])

    return (
        <React.Fragment>

            <Row className={"pb-3"}>
                <Col xs={"auto"}>
                    <Button ariaLabel={"Select data to query"}
                            className={"my-4 min-width-px-150"}
                            isDispatched={false}
                            onClick={onToggleImportModal}
                            variant={"info"}
                    >
                        Select dataset
                    </Button>

                </Col>
            </Row>

            <SelectDatasetModal show={showChooseDataModal}
                                toggle={onToggleImportModal}
            />
        </React.Fragment>
    )
};

export default OpenModalToSelectDataset;