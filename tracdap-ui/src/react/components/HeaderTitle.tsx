/**
 * A component that shows a header title (H1, H2 etc.). This component allows tooltips and more layout options
 * to be rendered, the {@link HeaderSelector} component is simpler if this is not needed.
 *
 * @module HeaderTitle
 * @category Component
 */

import {HeaderSelector} from "./HeaderSelector";
import {Icon} from "./Icon"
import PropTypes from "prop-types";
import React, {memo} from "react";

/**
 * An interface for the props of the HeaderTitle component.
 */
export interface Props {

    /**
     * The css class to apply to the header tag, this allows additional styles to be added to the inner component.
     */
    headerClassName?: string,
    /**
     * The css class to apply to the title, this allows additional styles to be added to the outer component.
     */
    outerClassName?: string,
    /**
     * The text to show in the header.
     */
    text: string,
    /**
     * A message to show as a tooltip with the title.
     */
    tooltip?: string,
    /**
     * The html header type. h7 is just body text but the others have additional formatting applied.
     */
    type: "h1" | "h2" | "h3" | "h4" | "h5" | "h6" | 'h7'
}

const HeaderTitleInner = ({children, headerClassName, outerClassName, text, tooltip, type}: React.PropsWithChildren<Props>) => {

    // We use slightly default spacing on the header dependent on the size being used
    const finalClassName = `${outerClassName ? outerClassName : ["h1", "h2"].includes(type) ? "mt-5 mb-3" : "mt-4 mb-3"}`

    return (

        <div className={`d-flex align-items-center justify-content-between ${finalClassName}`}>
            <HeaderSelector className={headerClassName} type={type} text={text}>
                <React.Fragment>
                    {tooltip &&
                        <Icon ariaLabel={"More information"}
                              className={"ms-2"}
                              icon={"bi-question-circle"}
                              tooltip={tooltip}
                        />
                    }
                </React.Fragment>
            </HeaderSelector>

            {children &&
                <div>
                    {children}
                </div>
            }
        </div>
    )
};

HeaderTitleInner.propTypes = {

    children: PropTypes.oneOfType([
        PropTypes.element,
        PropTypes.arrayOf(
            PropTypes.oneOfType([
                PropTypes.element
            ])
        )]),
    headerClassName: PropTypes.string,
    outerClassName: PropTypes.string,
    text: PropTypes.string.isRequired,
    tooltip: PropTypes.string,
    type: PropTypes.oneOf(['h2', 'h3', 'h4', 'h5', 'h6', 'h7']).isRequired
};

export const HeaderTitle = memo(HeaderTitleInner);