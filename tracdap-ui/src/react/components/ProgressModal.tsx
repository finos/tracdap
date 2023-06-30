/**
 * A component that shows a modal that blocks any navigation or other interaction until so background
 * process completes. For example this is used when streaming a data upload to TRAC. The component
 * shows a progress bar while up that can be updated.
 *
 * @module ProgressModal
 * @category Component
 */

import Col from "react-bootstrap/Col";
import {HeaderTitle} from "./HeaderTitle";
import {Loading} from "./Loading";
import Modal from "react-bootstrap/Modal";
import PropTypes from "prop-types";
import React from "react";
import Row from "react-bootstrap/Row";
import {ProgressBar} from "./ProgressBar";
import type {StreamingPayload} from "../../types/types_general";

// Some Bootstrap grid layouts
const lgGrid = {span: 8, offset: 2};
const mdGrid = {span: 10, offset: 1};
const xsGrid = {span: 12, offset: 0};

/**
 * An interface for the props of the ProgressModal component.
 */
export interface Props {

    /**
     * Whether the user can close the modal, this should be false while an uploading is happening.
     */
    allowUserToCloseModal: boolean
    /**
     * The css class to apply to the progress bar, this allows additional styles to be added to the component.
     */
    className?: string
    /**
     * The number of calls in the process that have competed.
     */
    completed: number
    /**
     * The text to show with the progress icon. If false then no text will show.
     */
    progressBarText?: string | false,
    /**
     * Whether to show the modal or not.
     */
    show: boolean
    /**
     * The text to show above the progress bar
     */
    modalText?: string,
    /**
     * The total number of calls in whatever process is running.
     */
    toDo: number
    /**
     * A function that toggles whether the modal is shown or not.
     */
    toggle: React.Dispatch<React.SetStateAction<StreamingPayload & { allowUserToCloseModal: boolean, show: boolean}>>
}

export const ProgressModal = (props: Props) => {

    const {allowUserToCloseModal, className, completed, modalText, progressBarText, show, toDo, toggle} = props

    return (

        <Modal show={show} backdrop={"static"} onHide={() => toggle((prevState) => ({...prevState, show: false}))}>

            <Modal.Header closeButton={allowUserToCloseModal}>
            </Modal.Header>

            <Modal.Body className={"mx-5"}>

                {modalText &&
                    <HeaderTitle headerClassName={"w-100 text-center"} text={modalText} type={"h3"}/>
                }
                <Row>
                    <Col className={"mt-1"} xs={xsGrid} md={mdGrid} lg={lgGrid}>

                        {toDo > 0 &&
                            <ProgressBar completed={completed} toDo={toDo} className={className} text={progressBarText}/>
                        }
                        {toDo === 0 &&
                            <Loading className={className} text={progressBarText}/>
                        }

                    </Col>
                </Row>

            </Modal.Body>

            <Modal.Footer>
            </Modal.Footer>
        </Modal>
    )
};

ProgressModal.propTypes = {

    show: PropTypes.bool.isRequired,
    toggle: PropTypes.func.isRequired
};