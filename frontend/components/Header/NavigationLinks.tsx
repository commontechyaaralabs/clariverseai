import React from 'react';

interface NavigationLinksProps {
  className?: string;
}

const NavigationLinks: React.FC<NavigationLinksProps> = ({ className = "" }) => {
  const links = [
    { href: "#solutions", label: "Solutions" },
    { href: "#expertise", label: "Our Expertise" },
    { href: "#contact", label: "Contact Us" }
  ];

  return (
    <div className={`flex items-center gap-8 ${className}`}>
      {links.map((link) => (
        <a
          key={link.href}
          href={link.href}
          className="text-white hover:text-pink-400 transition-colors"
        >
          {link.label}
        </a>
      ))}
    </div>
  );
};

export { NavigationLinks };
