/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package social.model;


public enum Category {

    SCIENCE("SC"), NEWS("NS"),
    BOOKS("BS"), TECHNOLOGY("TY"), SPORTS("ST"), ART("AT");
    
    public static final int CODE_LENGTH = ART.getCode().length();
    private final String code;

    private Category(String code) {
        this.code = code;
    }

    public String getCode() {
        return code;
    }
    

    public static Category forCode(String code) {
        for (Category c : Category.values()) {
            if (c.code.equals(code)) {
                return c;
            }
        }
        return null;
    }
}