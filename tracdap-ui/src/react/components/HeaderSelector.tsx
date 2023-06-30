/**
 * A component that shows a header title (H1, H2 etc.). This is a simple header, the {@link HeaderTitle} component
 * is more complex and allows tooltips and more layout options to be rendered.
 *
 * @module HeaderSelector
 * @category Component
 */

import PropTypes from "prop-types";
import React, {memo} from "react";

/**
 * An interface for the props of the HeaderSelector component.
 */
interface Props {

    /**
     * The css class to apply to the title, this allows additional styles to be added.
     */
    className?: string
    /**
     * The text to show in the header.
     */
    text: string
    /**
     * The html element type. h7 is just body text but the others have additional formatting applied.
     */
    type: "h1" | "h2" | "h3" | "h4" | "h5" | "h6" | "h7"
}

const HeaderSelectorInner = ({children, className, text, type}: React.PropsWithChildren<Props>) => {

    switch (type) {
        case "h1":
            return <h1 className={className}>{text}{children}</h1>
        case "h2":
            return <h2 className={className}>{text}{children}</h2>
        case "h3":
            return <h3 className={className}>{text}{children}</h3>
        case "h4":
            return <h4 className={className}>{text}{children}</h4>
        case "h5":
            return <h5 className={className}>{text}{children}</h5>
        case "h6":
            return <h6 className={className}>{text}{children}</h6>
        default:
            return <p className={className}>{text}{children}</p>
    }
};

HeaderSelectorInner.propTypes = {

    className: PropTypes.string,
    children: PropTypes.oneOfType([
        PropTypes.element,
        PropTypes.arrayOf(
            PropTypes.oneOfType([
                PropTypes.element
            ])
        )]),
    text: PropTypes.string.isRequired,
    tooltip: PropTypes.string,
    type: PropTypes.oneOf(['h2', 'h3', 'h4', 'h5', 'h6', 'h7']).isRequired
};

export const HeaderSelector = memo(HeaderSelectorInner);