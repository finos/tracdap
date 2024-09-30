/**
 * A component that shows a modal that allows the user to set which columns in a table are visible.
 *
 * @module ColumnVisibilityModal
 * @category Component
 */

import {Button} from "../Button";
import Col from "react-bootstrap/Col";
import Form from "react-bootstrap/Form";
import Modal from "react-bootstrap/Modal";
import PropTypes from "prop-types";
import React from "react";
import Row from "react-bootstrap/Row";
import type {Table} from "@tanstack/react-table";
import type {TableRow} from "../../../types/types_general";

// Some Bootstrap grid layouts
const lgGrid = {span: 8, offset: 2};
const mdGrid = {span: 10, offset: 1};
const xsGrid = {span: 12, offset: 0};

/**
 * An interface for the props of the ColumnVisibilityModal component.
 */
export interface Props {

    /**
     * Whether to show the modal or not.
     */
    show: boolean
    /**
     * The table object from the react-table plugin.
     */
    table: Table<TableRow>
    /**
     * A function that toggles whether the modal is shown or not.
     */
    toggle: React.Dispatch<React.SetStateAction<boolean>>
}

export const ColumnVisibilityModal = (props: Props) => {

    const {show, table, toggle} = props

    return (

        <Modal show={show} onHide={() => toggle(false)}>

            <Modal.Header closeButton>
                <Modal.Title>
                    Set which columns are visible
                </Modal.Title>
            </Modal.Header>

            <Modal.Body className={"mx-5"}>

                <Row>
                    <Col xs={xsGrid} md={mdGrid} lg={lgGrid}>

                        <Form.Check checked={table.getIsAllColumnsVisible()}
                                    className={"my-2 d-block"}
                                    id={"all"}
                                    label={"Toggle all"}
                                    onChange={table.getToggleAllColumnsVisibilityHandler()}
                                    type={"checkbox"}
                        />

                        <hr/>

                        {table.getAllLeafColumns().map(column => {
                            return (

                                <Form.Check checked={column.getIsVisible()}
                                            className={"mb-2 d-block"}
                                            key={column.id}
                                            id={column.id}
                                            label={`${column.columnDef.header}`}
                                            onChange={column.getToggleVisibilityHandler()}
                                            type={"checkbox"}
                                />

                            )
                        })}
                    </Col>
                </Row>
            </Modal.Body>

            <Modal.Footer>

                <Button ariaLabel={"Close column visibility menu"}
                        variant={"secondary"}
                        onClick={() => toggle(false)}
                        isDispatched={false}
                >
                    Close
                </Button>

            </Modal.Footer>
        </Modal>
    )
};

ColumnVisibilityModal.propTypes = {

    show: PropTypes.bool.isRequired,
    toggle: PropTypes.func.isRequired,
    table: PropTypes.object.isRequired
};