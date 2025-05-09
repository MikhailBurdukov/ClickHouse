#pragma once

#include <Parsers/ASTWithAlias.h>


namespace DB
{

/// Parameter in query with name and type of substitution ({name:type}).
/// Example: SELECT * FROM table WHERE id = {pid:UInt16}.
class ASTQueryParameter : public ASTWithAlias
{
public:
    String name;
    String type;

    ASTQueryParameter(const String & name_, const String & type_) : name(name_), type(type_) {}

    /** Get the text that identifies this element. */
    String getID(char delim) const override { return String("QueryParameter") + delim + name + ':' + type; }

    ASTPtr clone() const override;

    void updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const override;

protected:
    void formatImplWithoutAlias(WriteBuffer & ostr, const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
    void appendColumnNameImpl(WriteBuffer & ostr) const override;
};

}
