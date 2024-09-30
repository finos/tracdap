import {Style} from "@react-pdf/types";

const PdfCss: Record<string, Style> = {

    page: {
        paddingLeft: 60,
        paddingRight: 60,
        paddingTop: 60,
        paddingBottom: 60,
        flexDirection: "column",
        fontSize: 9,
    },
    title: {
        fontSize: 16,
        marginTop: 10,
        marginBottom: 15,
        fontFamily: "Helvetica-Bold"
    },
    text: {
        marginTop: 5,
        marginBottom: 5,
    },
    section: {
        flexDirection: "column",
        width: "auto",
        fontSize: 12,
        marginTop: 10,
        marginBottom: 5
    },
    subSection: {
        flexDirection: "column",
        width: "auto",
        fontSize: 10,
        marginTop: 10,
        marginBottom: 5,
        fontFamily: "Helvetica-Bold"
    },
    subSubSection: {
        flexDirection: "column",
        width: "auto",
        fontSize: 10,
        marginTop: 10,
        marginBottom: 5,
        fontFamily: "Helvetica"
    },
    table: {
        width: "auto",
        borderStyle: "solid",
        borderWidth: 0.5,
        borderTopColor: "#2a2a2c",
        borderBottomColor: "#2a2a2c",
        borderLeftWidth: 0,
        borderRightWidth: 0,
        marginTop: 10,
        marginBottom: 10
    },
    tableHead: {
        borderColor: "#2a2a2c",
        borderTopWidth: 0,
        borderBottomWidth: 0.5,
        fontFamily: "Helvetica-Bold",
        marginVertical: "auto",
    },
    tableRow: {
        margin: "auto",
        flexDirection: "row"
    },
    tableCol: {
        borderStyle: "solid",
        borderTopWidth: 0.5,
        borderLeftWidth: 0,
        borderRightWidth: 0,
        borderBottomWidth: 0,
        borderColor: "#dee2e6"
    },
    tableColTop: {
        borderTopWidth: 0
    },
    tableColBottom: {
        borderBottomWidth: 0
    },
    tableColFill: {
        backgroundColor: "#f9f9f9"
    },
    tableCell: {
        marginVertical: "auto",
        paddingTop: 3, paddingBottom: 3, fontSize: 8
    },
    footNotes: {
        fontSize: 7,
    },
    pageNumber: {
        width: "100%",
        textAlign: 'center',
        position: "absolute",
        left: 60,
        right: 0,
        bottom: 25
    },
    link: {
        color: "blue"
    },
    headerLogo: {
        position: "absolute", top: 30, right: 60
    }
}

export {PdfCss}