/**
 * A component that shows a large font title. If the component has children then these
 * are show on the right of the title.
 *
 * @module SceneTitle
 * @category Component
 */

import Col from "react-bootstrap/Col";
import PropTypes from "prop-types";
import React, {memo} from "react";
import Row from "react-bootstrap/Row";

/**
 * An interface for the props of the SceneTitle component.
 */
export interface Props {

    /**
     * The text to show as a title.
     */
    text: string
}

const SceneTitleInner = (props: React.PropsWithChildren<Props>) => {

    const {text, children} = props;

    return (

        <React.Fragment>
            {text ?
                <Row className={"mt-4 mb-3 spaced-text"}>
                    <Col>
                        <h2>{text}</h2>
                    </Col>
                    {children &&
                        <Col xs={'auto'} className={"float-end my-auto"}>
                            {children}
                        </Col>
                    }
                </Row>
                : null}
        </React.Fragment>
    )
};

SceneTitleInner.propTypes = {

    /**
     * The text to show as the title.
     */
    text: PropTypes.string.isRequired
};

export const SceneTitle = memo(SceneTitleInner);