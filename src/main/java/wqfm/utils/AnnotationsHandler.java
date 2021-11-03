package wqfm.utils;

import wqfm.configs.Config;
import wqfm.configs.DefaultValues;


/**
 *
 * @author mahim
 */
public class AnnotationsHandler {

    public static void handleAnnotations(String tree) {
        switch (Config.ANNOTATIONS_LEVEL) {
            case DefaultValues.ANNOTATIONS_LEVEL0_NONE:
                System.out.println("No annotations used.");
                break;

            case DefaultValues.ANNOTAIONS_LEVEL1_QUARTET_SUPPORT:
                System.out.println("Annotations done using quartet support.");
                AnnotationsHandler.handleQuartetSupport(tree);
                break;

            case DefaultValues.ANNOTATIONS_LEVEL2_QUARTET_SUPPORT_NORMALIZED_SUM:
                System.out.println("Annotations done after normalizing by sum, using quartet support");
                AnnotationsHandler.handleQuartetSupportNormalizedSum(tree);
                break;

            case DefaultValues.ANNOTATIONS_LEVEL3_QUARTET_SUPPORT_NORMALIZED_MAX:
                System.out.println("Annotations done after normalizing by max, using quartet support");
                AnnotationsHandler.handleQuartetSupportNormalizedMax(tree);
                break;
        }
    }

    private static void handleQuartetSupport(String tree) {
        String cmd = Config.PYTHON_ENGINE
                + " "
                + Config.BRANCH_ANNOTATIONS_QUARTET_SUPPORT
                + " "
                + Config.INPUT_FILE_NAME
                + " "
                + Config.SPECIES_TREE_FILE_NAME
                + " "
                + Config.OUTPUT_FILE_NAME;

        IOHandler.runSystemCommand(cmd);
    }

    private static void handleQuartetSupportNormalizedSum(String tree) {
        String cmd = Config.PYTHON_ENGINE
                + " "
                + Config.BRANCH_ANNOTATIONS_QUARTET_SUPPORT
                + " "
                + Config.INPUT_FILE_NAME
                + " "
                + Config.SPECIES_TREE_FILE_NAME
                + " "
                + Config.OUTPUT_FILE_NAME
                + " "
                + "sum";

        IOHandler.runSystemCommand(cmd);
    }

    private static void handleQuartetSupportNormalizedMax(String tree) {
        String cmd = Config.PYTHON_ENGINE
                + " "
                + Config.BRANCH_ANNOTATIONS_QUARTET_SUPPORT
                + " "
                + Config.INPUT_FILE_NAME
                + " "
                + Config.SPECIES_TREE_FILE_NAME
                + " "
                + Config.OUTPUT_FILE_NAME
                + " "
                + "max";

        IOHandler.runSystemCommand(cmd);
    }

    public static String GET_ANNOTATIONS_LEVEL_MESSAGE() {
        switch (Config.ANNOTATIONS_LEVEL) {
            case DefaultValues.ANNOTATIONS_LEVEL0_NONE:
                return "Using no annotations for branches";
            case DefaultValues.ANNOTAIONS_LEVEL1_QUARTET_SUPPORT:
                return "Using quartet support annotations for branches";
            case DefaultValues.ANNOTATIONS_LEVEL2_QUARTET_SUPPORT_NORMALIZED_SUM:
                return "Using quartet support normalized by sum annotations for branches";
            case DefaultValues.ANNOTATIONS_LEVEL3_QUARTET_SUPPORT_NORMALIZED_MAX:
                return "Using quartet support normalized by max annotations for branches";
            default:
                return "Using no annotations for branches";
        }
    }

    public static String getAnnotationLevelMessages() {
        StringBuilder bld = new StringBuilder();
        bld.append("0: no annotations ");
        bld.append("1: annotations by quartet support ");
        bld.append("2: annotations by quartet support normalized by sum");
        bld.append("3: annotations by quartet support normalized by max");
        return bld.toString();
    }
}
