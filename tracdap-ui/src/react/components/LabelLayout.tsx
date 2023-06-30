/**
 * A component for applying a layout that allows a form element to have a label. This can either be
 * at the top of left (inline). Help text can also be added to the layout as well as tooltips.
 *
 * @module LabelLayout
 * @category Component
 */

import Form from "react-bootstrap/Form";
import {Icon} from "./Icon";
import PropTypes from "prop-types";
import React from "react";

/**
 * An interface for the props of the LabelLayout component.
 */
export interface Props {

    /**
     * The css class to apply to the label, this allows additional styles to be added to the layout.
     */
    className?: string
    /**
     * Helper text to show the user below the select component.
     */
    helperText?: string
    /**
     * An identifier for the form input, this is used to link the label to the form element.
     */
    id?: null | number | string
    /**
     * When showing a label this sets whether to show the label in a single row (left) with the select or a single column (above).
     */
    labelPosition?: "top" | "left"
    /**
     * A label to show with the select.
     */
    labelText?: string
    /**
     * An identifier for the select, used to give a unique ID to the helper text.
     */
    name?: string | number | null
    /**
     * Whether the select has a space included to show validation messages. If it does, we have to add some extra spaces to the label
     * layout to make it line up correctly.
     */
    showValidationMessage?: boolean
    /**
     * A tooltip to display with the label.
     */
    tooltip?: string
}

export const LabelLayout = (props: React.PropsWithChildren<Props>) => {

    const {
        children,
        className = "",
        helperText,
        id,
        labelText,
        labelPosition = "top",
        name,
        showValidationMessage,
        tooltip
    } = props

    // The icon to show if a tooltip is set, used in multiple places
    const TooltipIcon = <Icon ariaLabel={"Tooltip"} className={"ms-2"} icon={"bi-question-circle"} tooltip={tooltip}/>

    return (

        <React.Fragment>
            {/* If there is a label and the label is set to the left then define our own flex layout. */}
            {labelText != null && labelPosition === "left" &&
                <div className={`d-flex flex-column flex-sm-row ${className ? className : ""}`}>
                    <div className={"my-auto me-3 col-form-label py-sm-0"}>
                        {labelText || <br/>}
                        {tooltip && TooltipIcon}
                        {helperText &&
                            // This is purely a layout thing to push the label up if there is
                            // helper text - so that the layout looks more aligned
                            <div className={"d-none d-sm-block form-text"}>
                                <br/>
                            </div>
                        }
                        {showValidationMessage &&
                            // This is purely a layout thing to push the label up if there is
                            // a validation message space in the child. Note that this is
                            // not perfect as it assumes that the feedback message is on one line.
                            <Form.Control.Feedback type="invalid" className={"d-block"}>
                                <br/>
                            </Form.Control.Feedback>
                        }
                    </div>
                    <div className={"d-flex flex-column flex-fill"}>
                        <div>
                            {children}
                        </div>
                        {helperText &&

                            <div id={`${name}HelpBlock`} className={"form-text text-muted"}>
                                {helperText}
                            </div>
                        }
                    </div>
                </div>
            }

            {/* If there is a label and the label is set to the top then use the Bootstrap components for the layout, */}
            {/* we do not need to define our own flex layout. */}
            {labelText != null && labelPosition === "top" &&
                <Form.Group className={className}>

                    <Form.Label htmlFor={id?.toString()}>
                        {labelText || <br/>}
                        {tooltip && TooltipIcon}
                    </Form.Label>

                    <div className={"d-block"}>
                        {children}
                    </div>

                    {helperText &&
                        <Form.Text id={`${name}HelpBlock`} muted>
                            {helperText}
                        </Form.Text>
                    }

                </Form.Group>
            }

            {/*If there is no label text set then we just render the child component.*/}
            {labelText == null &&

                <React.Fragment>
                    {children}

                    {helperText &&
                        <Form.Text id={`${name}HelpBlock`} muted>
                            {helperText}
                        </Form.Text>
                    }
                </React.Fragment>
            }
        </React.Fragment>
    )
};

LabelLayout.propTypes = {

    className: PropTypes.string,
    helperText: PropTypes.string,
    labelPosition: PropTypes.oneOf(["top", "left"]),
    labelText: PropTypes.string,
    name: PropTypes.oneOfType([PropTypes.number, PropTypes.string]),
    showValidationMessage: PropTypes.bool,
    tooltip: PropTypes.string
};