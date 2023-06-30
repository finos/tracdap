/**
 * A component that shows block of text, this is used like a paragraph.
 *
 * @module TextBlock
 * @category Component
 */

import Col from "react-bootstrap/Col";
import PropTypes from "prop-types";
import React from "react";
import Row from "react-bootstrap/Row";

export interface Props {

    /**
     * The css class to apply to the text, this allows additional styles to be added.
     */
    className?: string
}

export const TextBlock = (props: React.PropsWithChildren<Props>) => {

    const {className, children} = props;

    // Note that the className overwrites the class rather than extending it
    const finalClassName= className ? `${className} spaced-text`  : "mt-3 mb-4 spaced-text"

    return (

        <Row>
            <Col className={finalClassName}>
                {children}
            </Col>
        </Row>
    )
};

TextBlock.propTypes = {

    className: PropTypes.string,
    children: PropTypes.oneOfType([PropTypes.element, PropTypes.string, PropTypes.arrayOf(PropTypes.node)]).isRequired
};